//! Thin Node projection of the Rust-owned multi-ontology lifecycle (#842).

use std::sync::{Arc, RwLock};

use graphforge_api::{
    ActivationProfileChangeRequest, BridgeAdoptionRequest, BridgeDeleteRequest,
    BridgeUpdateRequest, CancellationToken, CompositionChangeRequest, CompositionDataDisposition,
    GfError, GraphForge as ApiGraphForge, ModuleAdoptionRequest, ModuleDeleteRequest,
    ModuleUpdateRequest, MultiOntologyError, OntologyAuthorityExpectation,
    ResolutionExplainRequest, WriteContext,
};
use napi::Env;
use napi::Task;
use napi::bindgen_prelude::{AbortSignal, AsyncTask, Unknown};
use napi_derive::napi;
use serde_json::Value;

use crate::{GraphForge, NodeError, Result, canonical_operation_id, optional_uuid, to_napi_err};

#[napi(object)]
/// Exact authority expectation supplied to a mutating operation.
pub struct AuthorityInput {
    /// Idempotency UUID.
    pub operation_uuid: String,
    /// Optional actor UUID.
    pub actor_uuid: Option<String>,
    /// Exact current project generation UUID.
    pub expected_project_generation_uuid: String,
    /// Exact current composition fingerprint.
    pub expected_composition_fingerprint: Option<String>,
}

#[napi(object)]
/// Input for creating a non-authoritative module candidate.
pub struct ModuleCandidateInput {
    /// Authored module document.
    pub document: Value,
    /// Exact module dependencies.
    pub dependencies: Option<Vec<Value>>,
    /// Optional activation override.
    pub enforcement: Option<String>,
}

#[napi(object)]
/// Input for parsing a non-authoritative module candidate.
pub struct ModuleImportInput {
    /// Owned document text.
    pub text: String,
    /// Input format token.
    pub format: String,
    /// Exact module dependencies.
    pub dependencies: Option<Vec<Value>>,
}

#[napi(object, object_to_js = false)]
/// Owned module mutation input.
pub struct ModuleMutationInput {
    /// Exact authority expectation.
    pub authority: AuthorityInput,
    /// Exact or uniquely resolving selector.
    pub selector: Option<Value>,
    /// Candidate returned by create or import.
    pub candidate: Option<Value>,
    /// Replacement document.
    pub document: Option<Value>,
    /// Replacement dependencies.
    pub dependencies: Option<Vec<Value>>,
    /// Optional activation override.
    pub enforcement: Option<String>,
    /// Optional standard cancellation signal.
    pub signal: Option<AbortSignal>,
}

#[napi(object)]
/// Input for parsing a non-authoritative bridge candidate.
pub struct BridgeImportInput {
    /// Owned bridge document text.
    pub text: String,
    /// Input format token.
    pub format: String,
}

#[napi(object, object_to_js = false)]
/// Owned bridge mutation input.
pub struct BridgeMutationInput {
    /// Exact authority expectation.
    pub authority: AuthorityInput,
    /// Exact or uniquely resolving selector.
    pub selector: Option<Value>,
    /// Candidate returned by create or import.
    pub candidate: Option<Value>,
    /// Replacement bridge document.
    pub document: Option<Value>,
    /// Optional standard cancellation signal.
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
/// Complete activation-profile replacement input.
pub struct ActivationChangeInput {
    /// Exact authority expectation.
    pub authority: AuthorityInput,
    /// New default activation mode.
    pub profile_default: String,
    /// Complete replacement activation records.
    pub activation: Vec<Value>,
    /// Optional standard cancellation signal.
    pub signal: Option<AbortSignal>,
}

#[napi(object, object_to_js = false)]
/// Full composition-preflight input.
pub struct CompositionPreflightInput {
    /// Exact authority expectation.
    pub authority: AuthorityInput,
    /// Complete candidate composition.
    pub candidate: Value,
    /// Optional standard cancellation signal.
    pub signal: Option<AbortSignal>,
}

#[napi(object)]
/// Rust-owned resolution explanation input.
pub struct ResolutionInput {
    /// Optional exact module identity.
    pub module: Option<Value>,
    /// Symbol-kind token.
    pub kind: String,
    /// Local symbol identifier.
    pub local_id: String,
    /// Bounded ambiguity candidate count.
    pub max_candidates: Option<u32>,
}

fn decode<T: serde::de::DeserializeOwned>(value: Value, subject: &str) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|error| to_napi_err(&GfError::Validation(format!("invalid {subject}: {error}"))))
}

fn encode<T: serde::Serialize>(value: T, subject: &str) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|error| to_napi_err(&GfError::Validation(format!("encode {subject}: {error}"))))
}

fn module_selector(value: Value) -> Result<graphforge_api::ModuleSelector> {
    let Value::Object(mut input) = value else {
        return Err(to_napi_err(&GfError::Validation(
            "ontology module selector must be an object".into(),
        )));
    };
    match (
        input.remove("exact"),
        input.remove("ontologyId"),
        input.is_empty(),
    ) {
        (Some(exact), None, true) => Ok(graphforge_api::ModuleSelector::Exact(decode(
            exact,
            "exact ontology module identity",
        )?)),
        (None, Some(Value::String(ontology_id)), true) => {
            Ok(graphforge_api::ModuleSelector::OntologyId(ontology_id))
        }
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology module selector must contain exactly one of exact or ontologyId".into(),
        ))),
    }
}

fn bridge_selector(value: Value) -> Result<graphforge_api::BridgeSelector> {
    let Value::Object(mut input) = value else {
        return Err(to_napi_err(&GfError::Validation(
            "ontology bridge selector must be an object".into(),
        )));
    };
    match (
        input.remove("exact"),
        input.remove("bridgeId"),
        input.is_empty(),
    ) {
        (Some(exact), None, true) => Ok(graphforge_api::BridgeSelector::Exact(decode(
            exact,
            "exact ontology bridge identity",
        )?)),
        (None, Some(Value::String(bridge_id)), true) => {
            Ok(graphforge_api::BridgeSelector::BridgeId(bridge_id))
        }
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology bridge selector must contain exactly one of exact or bridgeId".into(),
        ))),
    }
}

fn authority(input: AuthorityInput) -> Result<OntologyAuthorityExpectation> {
    let expected_project_generation_uuid =
        uuid::Uuid::parse_str(&input.expected_project_generation_uuid).map_err(|_| {
            to_napi_err(&GfError::Validation(
                "invalid expected project generation UUID".into(),
            ))
        })?;
    if expected_project_generation_uuid.hyphenated().to_string()
        != input.expected_project_generation_uuid
    {
        return Err(to_napi_err(&GfError::Validation(
            "expected project generation UUID must be canonical hyphenated text".into(),
        )));
    }
    Ok(OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: canonical_operation_id(&input.operation_uuid)?,
            actor_uuid: optional_uuid(input.actor_uuid.as_deref())?,
        },
        expected_project_generation_uuid,
        expected_composition_fingerprint: input.expected_composition_fingerprint,
    })
}

fn activation_mode(value: &str) -> Result<graphforge_api::ActivationMode> {
    match value {
        "exploratory" => Ok(graphforge_api::ActivationMode::Exploratory),
        "advisory" => Ok(graphforge_api::ActivationMode::Advisory),
        "strict" => Ok(graphforge_api::ActivationMode::Strict),
        _ => Err(to_napi_err(&GfError::Validation(
            "activation mode must be exploratory, advisory, or strict".into(),
        ))),
    }
}

fn module_format(value: &str) -> Result<graphforge_api::ImportFormatHint> {
    match value {
        "auto" => Ok(graphforge_api::ImportFormatHint::Auto),
        "json" => Ok(graphforge_api::ImportFormatHint::Json),
        "yaml" => Ok(graphforge_api::ImportFormatHint::Yaml),
        _ => Err(to_napi_err(&GfError::Validation(
            "module import format must be auto, json, or yaml".into(),
        ))),
    }
}

fn bridge_format(value: &str) -> Result<graphforge_api::BridgeImportFormatHint> {
    match value {
        "auto" => Ok(graphforge_api::BridgeImportFormatHint::Auto),
        "json" => Ok(graphforge_api::BridgeImportFormatHint::Json),
        "yaml" => Ok(graphforge_api::BridgeImportFormatHint::Yaml),
        _ => Err(to_napi_err(&GfError::Validation(
            "bridge import format must be auto, json, or yaml".into(),
        ))),
    }
}

fn export_format(value: &str) -> Result<graphforge_api::ExportFormat> {
    match value {
        "json" => Ok(graphforge_api::ExportFormat::Json),
        "yaml" => Ok(graphforge_api::ExportFormat::Yaml),
        _ => Err(to_napi_err(&GfError::Validation(
            "module export format must be json or yaml".into(),
        ))),
    }
}

fn bridge_export_format(value: &str) -> Result<graphforge_api::BridgeExportFormat> {
    match value {
        "json" => Ok(graphforge_api::BridgeExportFormat::Json),
        "yaml" => Ok(graphforge_api::BridgeExportFormat::Yaml),
        _ => Err(to_napi_err(&GfError::Validation(
            "bridge export format must be json or yaml".into(),
        ))),
    }
}

fn symbol_kind(value: &str) -> Result<graphforge_api::SymbolKind> {
    match value {
        "entity" => Ok(graphforge_api::SymbolKind::Entity),
        "relation" => Ok(graphforge_api::SymbolKind::Relation),
        "property" => Ok(graphforge_api::SymbolKind::Property),
        _ => Err(to_napi_err(&GfError::Validation(
            "symbol kind must be entity, relation, or property".into(),
        ))),
    }
}

fn bind_signal(signal: Option<AbortSignal>) -> CancellationToken {
    let cancellation = CancellationToken::new();
    if let Some(signal) = signal {
        let bound = cancellation.clone();
        signal.on_abort(move || bound.cancel());
    }
    cancellation
}

fn to_multi_napi_err(error: &MultiOntologyError) -> NodeError {
    let reason = serde_json::to_string(error)
        .unwrap_or_else(|_| format!("{{\"code\":\"{}\"}}", error.code()));
    napi::Error::new(error.code().to_owned(), reason)
}

fn to_multi_deferred_err(env: Env, error: &MultiOntologyError) -> napi::Error {
    let value = napi::JsError::from(to_multi_napi_err(error)).into_unknown(env);
    napi::Error::from(value)
}

enum Mutation {
    AdoptModule(ModuleAdoptionRequest),
    UpdateModule(ModuleUpdateRequest),
    DeleteModule(ModuleDeleteRequest),
    AdoptBridge(BridgeAdoptionRequest),
    UpdateBridge(BridgeUpdateRequest),
    DeleteBridge(BridgeDeleteRequest),
    ChangeActivation(ActivationProfileChangeRequest),
    AdoptPortable(OntologyAuthorityExpectation),
}

/// Deferred Rust-owned multi-ontology mutation.
pub struct MultiOntologyMutationTask {
    engine: Arc<RwLock<ApiGraphForge>>,
    mutation: Option<Mutation>,
    cancellation: CancellationToken,
}

impl Task for MultiOntologyMutationTask {
    type Output =
        std::result::Result<graphforge_api::MultiOntologyMutationReceipt, MultiOntologyError>;
    type JsValue = Unknown<'static>;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let mut graph = self
            .engine
            .write()
            .map_err(|_| napi::Error::from_reason("GraphForge lock poisoned"))?;
        let mutation = self
            .mutation
            .take()
            .ok_or_else(|| napi::Error::from_reason("multi-ontology mutation already consumed"))?;
        let cancellation = Some(&self.cancellation);
        Ok(match mutation {
            Mutation::AdoptModule(request) => graph.adopt_ontology_module(&request, cancellation),
            Mutation::UpdateModule(request) => graph.update_ontology_module(&request, cancellation),
            Mutation::DeleteModule(request) => graph.delete_ontology_module(&request, cancellation),
            Mutation::AdoptBridge(request) => graph.adopt_ontology_bridge(&request, cancellation),
            Mutation::UpdateBridge(request) => graph.update_ontology_bridge(&request, cancellation),
            Mutation::DeleteBridge(request) => graph.delete_ontology_bridge(&request, cancellation),
            Mutation::ChangeActivation(request) => {
                graph.change_ontology_activation_profile(&request, cancellation)
            }
            Mutation::AdoptPortable(authority) => graph.adopt_portable_ontology_staging(
                &authority,
                graphforge_api::PortableV2Limits::default(),
                cancellation,
            ),
        })
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        let receipt = output.map_err(|error| to_multi_deferred_err(env, &error))?;
        env.to_js_value(&receipt)
    }
}

fn mutation_task(
    graph: &GraphForge,
    mutation: Mutation,
    signal: Option<AbortSignal>,
) -> Result<AsyncTask<MultiOntologyMutationTask>> {
    graph.ensure_open()?;
    Ok(AsyncTask::new(MultiOntologyMutationTask {
        engine: Arc::clone(&graph.inner),
        mutation: Some(mutation),
        cancellation: bind_signal(signal),
    }))
}

#[napi]
impl GraphForge {
    /// Return the exact authority identities required by every mutation.
    #[napi]
    pub fn ontology_authority_state(&self) -> Result<Value> {
        encode(
            self.open_guard()?
                .ontology_authority_state()
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology authority state",
        )
    }

    #[napi]
    /// List authoritative modules in Rust-defined order.
    pub fn ontology_modules(&self) -> Result<Value> {
        encode(
            self.open_guard()?
                .ontology_modules()
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology modules",
        )
    }

    #[napi]
    /// Inspect one authoritative module using a Rust selector.
    pub fn inspect_ontology_module(&self, selector: Value) -> Result<Value> {
        let selector = module_selector(selector)?;
        encode(
            self.open_guard()?
                .inspect_ontology_module(&selector)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module inspection",
        )
    }

    #[napi]
    /// Validate an authored module without mutation.
    pub fn validate_ontology_module(&self, document: Value) -> Result<Value> {
        let document = decode(document, "ontology module document")?;
        encode(
            self.open_guard()?
                .validate_ontology_module(&document)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module validation receipt",
        )
    }

    #[napi]
    /// Create a validated, non-authoritative module candidate.
    pub fn create_ontology_module(&self, input: ModuleCandidateInput) -> Result<Value> {
        let document = decode(input.document, "ontology module document")?;
        let dependencies = input
            .dependencies
            .unwrap_or_default()
            .into_iter()
            .map(|v| decode(v, "ontology module dependency"))
            .collect::<Result<Vec<_>>>()?;
        let enforcement = input
            .enforcement
            .as_deref()
            .map(activation_mode)
            .transpose()?;
        encode(
            self.open_guard()?
                .create_ontology_module(document, dependencies, enforcement)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module candidate",
        )
    }

    #[napi]
    /// Import a parsed, non-authoritative module candidate.
    pub fn import_ontology_module(&self, input: ModuleImportInput) -> Result<Value> {
        let dependencies = input
            .dependencies
            .unwrap_or_default()
            .into_iter()
            .map(|v| decode(v, "ontology module dependency"))
            .collect::<Result<Vec<_>>>()?;
        encode(
            self.open_guard()?
                .import_ontology_module(&input.text, module_format(&input.format)?, dependencies)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module candidate",
        )
    }

    #[napi]
    /// Explicitly adopt a module candidate.
    pub fn adopt_ontology_module(
        &self,
        mut input: ModuleMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = ModuleAdoptionRequest {
            authority: authority(input.authority)?,
            candidate: decode(
                input.candidate.take().ok_or_else(|| {
                    to_napi_err(&GfError::Validation("candidate is required".into()))
                })?,
                "ontology module candidate",
            )?,
        };
        mutation_task(self, Mutation::AdoptModule(request), input.signal.take())
    }

    #[napi]
    /// Preview an exact module replacement.
    pub fn preview_update_ontology_module(
        &self,
        selector: Value,
        document: Value,
        dependencies: Option<Vec<Value>>,
    ) -> Result<Value> {
        let selector = module_selector(selector)?;
        let document = decode(document, "ontology module document")?;
        let dependencies = dependencies
            .unwrap_or_default()
            .into_iter()
            .map(|v| decode(v, "ontology module dependency"))
            .collect::<Result<Vec<_>>>()?;
        encode(
            self.open_guard()?
                .preview_update_ontology_module(&selector, &document, &dependencies)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module update preview",
        )
    }

    #[napi]
    /// Atomically replace one module.
    pub fn update_ontology_module(
        &self,
        mut input: ModuleMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = ModuleUpdateRequest {
            authority: authority(input.authority)?,
            selector: module_selector(input.selector.take().ok_or_else(|| {
                to_napi_err(&GfError::Validation("selector is required".into()))
            })?)?,
            document: decode(
                input.document.take().ok_or_else(|| {
                    to_napi_err(&GfError::Validation("document is required".into()))
                })?,
                "ontology module document",
            )?,
            dependencies: input
                .dependencies
                .take()
                .unwrap_or_default()
                .into_iter()
                .map(|v| decode(v, "ontology module dependency"))
                .collect::<Result<Vec<_>>>()?,
            enforcement: input
                .enforcement
                .as_deref()
                .map(activation_mode)
                .transpose()?,
        };
        mutation_task(self, Mutation::UpdateModule(request), input.signal.take())
    }

    #[napi]
    /// Preview dependency and activation blockers for module deletion.
    pub fn preview_delete_ontology_module(&self, selector: Value) -> Result<Value> {
        let selector = module_selector(selector)?;
        encode(
            self.open_guard()?
                .preview_delete_ontology_module(&selector)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology module delete preview",
        )
    }

    #[napi]
    /// Atomically delete one safe module.
    pub fn delete_ontology_module(
        &self,
        mut input: ModuleMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = ModuleDeleteRequest {
            authority: authority(input.authority)?,
            selector: module_selector(input.selector.take().ok_or_else(|| {
                to_napi_err(&GfError::Validation("selector is required".into()))
            })?)?,
        };
        mutation_task(self, Mutation::DeleteModule(request), input.signal.take())
    }

    #[napi]
    /// Deterministically export one module.
    pub fn export_ontology_module(&self, selector: Value, format: String) -> Result<String> {
        let selector = module_selector(selector)?;
        self.open_guard()?
            .export_ontology_module(&selector, export_format(&format)?)
            .map_err(|error| to_multi_napi_err(&error))
    }

    #[napi]
    /// List authoritative bridge sets in Rust-defined order.
    pub fn ontology_bridges(&self) -> Result<Value> {
        encode(
            self.open_guard()?
                .ontology_bridges()
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridges",
        )
    }

    #[napi]
    /// Inspect one authoritative bridge set using a Rust selector.
    pub fn inspect_ontology_bridge(&self, selector: Value) -> Result<Value> {
        let selector = bridge_selector(selector)?;
        encode(
            self.open_guard()?
                .inspect_ontology_bridge(&selector)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge inspection",
        )
    }

    #[napi]
    /// Validate an authored bridge document without mutation.
    pub fn validate_ontology_bridge(&self, document: Value) -> Result<Value> {
        let document = decode(document, "ontology bridge document")?;
        encode(
            self.open_guard()?
                .validate_ontology_bridge(&document)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge validation receipt",
        )
    }

    #[napi]
    /// Create a validated, non-authoritative bridge candidate.
    pub fn create_ontology_bridge(&self, document: Value) -> Result<Value> {
        let document = decode(document, "ontology bridge document")?;
        encode(
            self.open_guard()?
                .create_ontology_bridge(document)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge candidate",
        )
    }

    #[napi]
    /// Import a parsed, non-authoritative bridge candidate.
    pub fn import_ontology_bridge(&self, input: BridgeImportInput) -> Result<Value> {
        encode(
            self.open_guard()?
                .import_ontology_bridge(&input.text, bridge_format(&input.format)?)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge candidate",
        )
    }

    #[napi]
    /// Explicitly adopt a bridge candidate.
    pub fn adopt_ontology_bridge(
        &self,
        mut input: BridgeMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = BridgeAdoptionRequest {
            authority: authority(input.authority)?,
            candidate: decode(
                input.candidate.take().ok_or_else(|| {
                    to_napi_err(&GfError::Validation("candidate is required".into()))
                })?,
                "ontology bridge candidate",
            )?,
        };
        mutation_task(self, Mutation::AdoptBridge(request), input.signal.take())
    }

    #[napi]
    /// Preview an exact bridge replacement.
    pub fn preview_update_ontology_bridge(
        &self,
        selector: Value,
        document: Value,
    ) -> Result<Value> {
        let selector = bridge_selector(selector)?;
        let document = decode(document, "ontology bridge document")?;
        encode(
            self.open_guard()?
                .preview_update_ontology_bridge(&selector, &document)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge update preview",
        )
    }

    #[napi]
    /// Atomically replace one bridge set.
    pub fn update_ontology_bridge(
        &self,
        mut input: BridgeMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = BridgeUpdateRequest {
            authority: authority(input.authority)?,
            selector: bridge_selector(input.selector.take().ok_or_else(|| {
                to_napi_err(&GfError::Validation("selector is required".into()))
            })?)?,
            document: decode(
                input.document.take().ok_or_else(|| {
                    to_napi_err(&GfError::Validation("document is required".into()))
                })?,
                "ontology bridge document",
            )?,
        };
        mutation_task(self, Mutation::UpdateBridge(request), input.signal.take())
    }

    #[napi]
    /// Preview dependency and activation blockers for bridge deletion.
    pub fn preview_delete_ontology_bridge(&self, selector: Value) -> Result<Value> {
        let selector = bridge_selector(selector)?;
        encode(
            self.open_guard()?
                .preview_delete_ontology_bridge(&selector)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology bridge delete preview",
        )
    }

    #[napi]
    /// Atomically delete one safe bridge set.
    pub fn delete_ontology_bridge(
        &self,
        mut input: BridgeMutationInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = BridgeDeleteRequest {
            authority: authority(input.authority)?,
            selector: bridge_selector(input.selector.take().ok_or_else(|| {
                to_napi_err(&GfError::Validation("selector is required".into()))
            })?)?,
        };
        mutation_task(self, Mutation::DeleteBridge(request), input.signal.take())
    }

    #[napi]
    /// Deterministically export one bridge set.
    pub fn export_ontology_bridge(&self, selector: Value, format: String) -> Result<String> {
        let selector = bridge_selector(selector)?;
        self.open_guard()?
            .export_ontology_bridge(&selector, bridge_export_format(&format)?)
            .map_err(|error| to_multi_napi_err(&error))
    }

    #[napi]
    /// Inspect the complete activation profile.
    pub fn ontology_activation_profile(&self) -> Result<Value> {
        let (profile_default, activation) = self
            .open_guard()?
            .ontology_activation_profile()
            .map_err(|error| to_multi_napi_err(&error))?;
        encode(
            serde_json::json!({ "profile_default": profile_default, "activation": activation }),
            "ontology activation profile",
        )
    }

    #[napi]
    /// Atomically replace the complete activation profile.
    pub fn change_ontology_activation_profile(
        &self,
        mut input: ActivationChangeInput,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        let request = ActivationProfileChangeRequest {
            authority: authority(input.authority)?,
            profile_default: activation_mode(&input.profile_default)?,
            activation: input
                .activation
                .drain(..)
                .map(|v| decode(v, "ontology activation record"))
                .collect::<Result<Vec<_>>>()?,
        };
        mutation_task(
            self,
            Mutation::ChangeActivation(request),
            input.signal.take(),
        )
    }

    #[napi]
    /// Validate and inventory a complete composition.
    pub fn validate_ontology_composition(&self, candidate: Value) -> Result<Value> {
        let candidate = decode(candidate, "ontology composition")?;
        encode(
            self.open_guard()?
                .validate_ontology_composition(&candidate)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology composition validation",
        )
    }

    #[napi]
    /// Run the Rust-owned stored-data and portability preflight.
    pub fn preflight_ontology_composition(
        &self,
        mut input: CompositionPreflightInput,
    ) -> Result<Value> {
        let authority = authority(input.authority)?;
        let request = CompositionChangeRequest {
            context: authority.context,
            expected_project_generation_uuid: authority.expected_project_generation_uuid,
            expected_composition_fingerprint: authority.expected_composition_fingerprint,
            candidate: decode(input.candidate, "ontology composition")?,
            data_disposition: CompositionDataDisposition::RequireConformingOperation {
                operation: "composition.preflight".into(),
            },
        };
        let cancellation = bind_signal(input.signal.take());
        encode(
            self.open_guard()?
                .preflight_ontology_composition(&request, Some(&cancellation))
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology composition preflight",
        )
    }

    #[napi]
    /// Explain qualified or unqualified symbol resolution.
    pub fn explain_ontology_resolution(&self, input: ResolutionInput) -> Result<Value> {
        let request = ResolutionExplainRequest {
            module: input
                .module
                .map(|v| decode(v, "ontology module identity"))
                .transpose()?,
            kind: symbol_kind(&input.kind)?,
            local_id: input.local_id,
            max_candidates: input.max_candidates.unwrap_or(16) as usize,
        };
        encode(
            self.open_guard()?
                .explain_ontology_resolution(&request)
                .map_err(|error| to_multi_napi_err(&error))?,
            "ontology resolution explanation",
        )
    }

    #[napi]
    /// Inspect the verified non-authoritative portable ontology staging record.
    pub fn portable_ontology_staging(&self) -> Result<Value> {
        encode(
            self.open_guard()?
                .portable_ontology_staging(graphforge_api::PortableV2Limits::default())
                .map_err(|error| to_multi_napi_err(&error))?,
            "portable ontology staging",
        )
    }

    #[napi]
    /// Explicitly adopt verified portable ontology staging.
    pub fn adopt_portable_ontology_staging(
        &self,
        authority_input: AuthorityInput,
        signal: Option<AbortSignal>,
    ) -> Result<AsyncTask<MultiOntologyMutationTask>> {
        mutation_task(
            self,
            Mutation::AdoptPortable(authority(authority_input)?),
            signal,
        )
    }
}
