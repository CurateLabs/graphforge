//! Query binding, execution, streaming, and result sinks for the public facade.

use arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use graphforge_ir::{
    BindError, Binder, CompositionBindingContext, GraphOp, GraphPlan, IrExpr, ProcedureRegistry,
    RuntimeCatalog,
};
use graphforge_ontology::OntologyHandle;
use graphforge_storage::GraphCatalog;

use super::result_shaping::{shape_result, shape_stream};
use super::{
    ApiErrorCode, CancellationToken, ExecutionResult, GfError, GraphForge, GraphWorkspace,
    IrLiteral, LoweringError, OntologyMode, ProcedureDefinition, ProjectErrorCode,
    ResultSinkFormat, ResultSinkOptions, ResultSinkReceipt, RuntimeGuard,
    SendableRecordBatchStream, Span, adjacency_provider_for_graph, mutation_transaction,
};

impl GraphForge {
    pub(super) fn execute_read_only(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let plan = Binder::new(
            self.ontology.clone(),
            self.runtime_catalog.clone(),
            self.ontology_mode,
        )
        .with_procedures(self.procedure_snapshot())
        .bind(&ast)
        .map_err(|errs| bind_errors_to_gferror(&errs))?;
        if plan.ops.iter().any(|op| {
            matches!(
                op,
                GraphOp::Create { .. }
                    | GraphOp::Merge { .. }
                    | GraphOp::Delete { .. }
                    | GraphOp::Set { .. }
                    | GraphOp::Remove { .. }
            )
        }) {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "checkpoint views are read-only".into(),
            });
        }
        shape_result(
            self.run_plan(&plan, &HashMap::new())?,
            self.ontology_mode,
            self.ontology.as_ref(),
        )
    }

    /// Execute an openCypher query and return its Arrow-backed result.
    ///
    /// Runs the full pipeline: `parse → bind → lower → execute`. A query
    /// containing `CREATE` writes through [`graphforge_exec::ExecutionSession::execute_create`];
    /// a read query runs through `execute_plan`. The result exposes UUID
    /// identity columns (`node_uuid`/`edge_uuid`) — never internal surrogate
    /// scan keys — while preserving legal user aliases such as
    /// `RETURN id(n) AS node_id` (#703). The schema carries query metadata.
    ///
    /// # Errors
    /// Returns [`GfError::Parse`] on a parse failure, [`GfError::Plan`] on a bind
    /// failure (e.g. a strict-mode unknown label), and [`GfError::Plan`] /
    /// [`GfError::Execution`] on lowering / execution failures.
    pub fn execute(&self, cypher: &str) -> Result<ExecutionResult, GfError> {
        self.execute_with_params(cypher, &HashMap::new())
    }

    /// Register or replace a deterministic procedure available to `CALL`.
    ///
    /// # Errors
    /// Returns [`GfError::Validation`] when a fixture row does not match the
    /// declared input and output width.
    pub fn register_procedure(&self, procedure: ProcedureDefinition) -> Result<(), GfError> {
        let width = procedure.inputs.len() + procedure.outputs.len();
        if let Some(row) = procedure.rows.iter().find(|row| row.len() != width) {
            return Err(GfError::Validation(format!(
                "procedure {} expects {width} fixture columns, found {}",
                procedure.name,
                row.len()
            )));
        }
        self.procedures
            .lock()
            .expect("procedure registry poisoned")
            .insert(procedure.name.clone(), procedure);
        Ok(())
    }

    pub(super) fn procedure_snapshot(&self) -> Arc<ProcedureRegistry> {
        Arc::new(
            self.procedures
                .lock()
                .expect("procedure registry poisoned")
                .clone(),
        )
    }

    /// Execute an openCypher query with bind-time parameters.
    ///
    /// See [`execute`](Self::execute); `params` supplies values for `$name`
    /// placeholders in the query.
    ///
    /// # Errors
    /// As [`execute`](Self::execute).
    pub fn execute_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query(cypher, params)
    }

    /// Execute a query using exact compiled multi-ontology binding authority.
    ///
    /// The composition is consumed by the same parse, bind, lower, and execute
    /// path as [`execute`](Self::execute). Its fingerprint and deterministic
    /// binding receipts are retained in the plan; runtime-catalog observations
    /// are published only after the complete bind succeeds.
    ///
    /// # Errors
    /// As [`execute`](Self::execute), including scoped composition diagnostics.
    pub fn execute_with_composition(
        &self,
        cypher: &str,
        composition: Arc<CompositionBindingContext>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_composition(cypher, &HashMap::new(), composition, true)
    }

    fn run_query(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_publish(cypher, params, true)
    }

    /// Execute a write Cypher statement against the private workspace without
    /// moving `CURRENT`. Used by the uniform transaction lifecycle so multiple
    /// staged writers share one later publication.
    pub(crate) fn execute_write_without_publish(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_publish(cypher, params, false)
    }

    fn run_query_with_publish(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_optional_composition(
            cypher,
            params,
            self.default_composition_snapshot(),
            publish,
        )
    }

    pub(super) fn default_composition_snapshot(&self) -> Option<Arc<CompositionBindingContext>> {
        self.default_composition_context
            .lock()
            .expect("default composition context lock poisoned")
            .clone()
    }

    pub(super) fn composition_execution_mode(context: &CompositionBindingContext) -> OntologyMode {
        // The binder owns fallback policy; resolved generation symbols retain
        // their typed storage route even under an exploratory profile.
        match context.composition().profile_default {
            graphforge_ontology::ActivationMode::Exploratory
            | graphforge_ontology::ActivationMode::Advisory => OntologyMode::Advisory,
            graphforge_ontology::ActivationMode::Strict => OntologyMode::Strict,
        }
    }

    pub(super) fn open_query_catalog(
        &self,
        runtime_catalog: &Arc<Mutex<RuntimeCatalog>>,
        candidate: Option<&graphforge_storage::SemanticStorageBindings>,
    ) -> Result<GraphCatalog, GfError> {
        let runtime = runtime_catalog.lock().expect("runtime catalog poisoned");
        let installed = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        GraphCatalog::open_authenticated_with_semantic_bindings(
            &self.dir(),
            self.ontology.as_ref(),
            &runtime,
            candidate.or(installed.as_ref()),
            self.property_inventory_for_session(),
        )
        .map_err(|error| GfError::Storage(error.to_string()))
    }

    fn run_query_with_composition(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        composition: Arc<CompositionBindingContext>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        self.run_query_with_optional_composition(cypher, params, Some(composition), publish)
    }

    fn run_query_with_optional_composition(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        composition: Option<Arc<CompositionBindingContext>>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        let _admission = self.admit_heavy_query()?;
        let composition = composition
            .map(|context| self.bind_generation_storage(&context))
            .transpose()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }

        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
        // A query that strips to zero clauses (e.g. comment-only or block-comment
        // -only) is empty even though its raw text is not blank, so the
        // `trim().is_empty()` guard above misses it. Reject it here rather than
        // letting the empty plan panic the result shaper (#603 — found by fuzz_exec).
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let is_mutation = ast.has_mutation_clauses();
        if is_mutation && self.read_only {
            return Err(GfError::Execution(
                "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
            ));
        }
        let _mutation_admission = (is_mutation && publish)
            .then(|| self.graph_visibility.lock())
            .transpose()?;
        let transaction = is_mutation.then(|| {
            graphforge_exec::mutation::MutationTransaction::new(
                &self
                    .runtime_catalog
                    .lock()
                    .expect("runtime catalog poisoned"),
            )
        });
        let binding_catalog = transaction.as_ref().map_or_else(
            || Arc::clone(&self.runtime_catalog),
            graphforge_exec::mutation::MutationTransaction::catalog,
        );
        validate_typed_parameter_binding(
            &ast,
            params,
            self.ontology.clone(),
            &binding_catalog,
            self.ontology_mode,
            self.procedure_snapshot(),
            composition
                .as_ref()
                .map(|(context, _, _)| Arc::clone(context)),
        )?;

        // Bind against the shared runtime catalog so newly observed types/props
        // persist across queries in this instance.
        let plan = {
            let mut binder = Binder::new(
                self.ontology.clone(),
                binding_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            if let Some((composition, _, _)) = &composition {
                binder = binder.with_composition(Arc::clone(composition));
            }
            binder
                .bind(&ast)
                .map_err(|errs| bind_errors_to_gferror(&errs))?
        };

        validate_call_params(&plan, params)?;

        let candidate = composition.as_ref().map(|(_, candidate, _)| candidate);
        let legacy_route_moves = composition.as_ref().map(|(_, _, moves)| moves.as_slice());
        let composition_mode = composition
            .as_ref()
            .map(|(context, _, _)| Self::composition_execution_mode(context));
        let result = self.run_plan_with_publish_and_bindings(
            &plan,
            params,
            publish,
            candidate,
            composition_mode,
            legacy_route_moves,
            transaction,
            is_mutation && publish,
        );
        let result = result.map_err(publicize_query_error)?;
        shape_result(result, self.ontology_mode, self.ontology.as_ref())
            .map_err(publicize_query_error)
    }

    /// Build a session reflecting the current runtime catalog and run `plan`,
    /// routing CREATE to the write path and reads to `execute_plan` with `$name`
    /// parameters substituted where the lowered logical plan still carries them.
    fn run_plan(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_plan_with_publish(plan, params, true)
    }

    fn run_plan_with_publish(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
    ) -> Result<ExecutionResult, GfError> {
        self.run_plan_with_publish_and_bindings(
            plan, params, publish, None, None, None, None, false,
        )
    }

    #[allow(clippy::too_many_lines)] // one visibility lock spans execution and publication
    #[allow(clippy::too_many_arguments)] // keep publication, binding and pre-admitted write context explicit
    fn run_plan_with_publish_and_bindings(
        &self,
        plan: &GraphPlan,
        params: &HashMap<String, IrLiteral>,
        publish: bool,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
        composition_mode: Option<OntologyMode>,
        legacy_route_moves: Option<&[(std::path::PathBuf, std::path::PathBuf)]>,
        transaction: Option<graphforge_exec::mutation::MutationTransaction>,
        write_admission_held: bool,
    ) -> Result<ExecutionResult, GfError> {
        use graphforge_exec::ExecutionSession;

        let plan = materialize_row_count_params(plan, params)?;

        // Route every supported write shape through the clause-ordered statement driver.
        let write_ops = plan
            .ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    GraphOp::Create { .. }
                        | GraphOp::Merge { .. }
                        | GraphOp::Delete { .. }
                        | GraphOp::Set { .. }
                        | GraphOp::Remove { .. }
                )
            })
            .count();
        let is_write = write_ops > 0;
        if is_write && self.read_only {
            return Err(GfError::Execution(
                "GF_WRITE_RESOURCE_READ_ONLY: session does not authorize writes".into(),
            ));
        }
        // Transaction commit already holds write admission when publish is false.
        let _write_visibility = (is_write && publish && !write_admission_held)
            .then(|| self.graph_visibility.lock())
            .transpose()?;
        let _read_visibility = (!is_write)
            .then(|| self.graph_visibility.read())
            .transpose()?;
        let workspace = self.workspace_for_session();
        let dir = workspace.path().to_path_buf();
        let prior_catalog = self
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone();
        let mut transaction = if is_write {
            Some(transaction.unwrap_or_else(|| {
                graphforge_exec::mutation::MutationTransaction::new(&prior_catalog)
            }))
        } else {
            None
        };
        let working_catalog = transaction.as_ref().map_or_else(
            || Arc::clone(&self.runtime_catalog),
            graphforge_exec::mutation::MutationTransaction::catalog,
        );
        let mut lifecycle = if is_write {
            Some(mutation_transaction::FacadeMutationLifecycle::new(
                self,
                prior_catalog,
                publish,
                candidate_bindings,
            )?)
        } else {
            None
        };
        let mut legacy_migration = None;
        if legacy_route_moves.is_some_and(|moves| !moves.is_empty()) {
            if !is_write || !publish {
                return Err(GfError::Validation(
                    "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: run a publishing write to migrate the unambiguous legacy generation".into(),
                ));
            }
            legacy_migration = Some(graphforge_storage::apply_legacy_route_moves(
                &dir,
                legacy_route_moves.expect("checked"),
                candidate_bindings.expect("legacy migration has candidate bindings"),
            )?);
        }

        // Open a catalog snapshot reflecting the freshly-bound runtime catalog so
        // read scans resolve property names interned during bind.
        let catalog = self.open_query_catalog(&working_catalog, candidate_bindings)?;
        // A compiled composition is explicit typed authority even when the
        // legacy workspace ontology profile remains exploratory. Its writes
        // must never fall back to `_untyped` host routing.
        let execution_mode = composition_mode.unwrap_or(self.ontology_mode);
        let adjacency_provider = if execution_mode == self.ontology_mode {
            self.adjacency_provider_for_session()
        } else {
            Arc::new(adjacency_provider_for_graph(
                &dir,
                execution_mode,
                self.property_inventory_for_session(),
            )?)
        };
        let session = ExecutionSession::new_with_target_provider_resources_and_identity(
            catalog,
            self.ontology.clone(),
            dir,
            execution_mode,
            adjacency_provider,
            Some(Arc::clone(&self.ordinal_identities)),
            &self.session_resource_config(),
        )?;
        let session = if self.read_only {
            session.restrict_to_reads()
        } else {
            session
        };

        let execution = self.block_on(async {
            if is_write {
                session
                    .prepare_write_statement_with_params(
                        &plan,
                        params,
                        transaction.as_mut().expect("write transaction"),
                    )
                    .await
            } else {
                session.execute_plan_with_params(&plan, params).await
            }
        });
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                if let Some(transaction) = transaction.take() {
                    return transaction.abort(lifecycle.as_mut().expect("write lifecycle"), error);
                }
                return Err(error);
            }
        };
        if let Some(transaction) = transaction.take() {
            let resource = session.write_resource()?;
            transaction.commit(
                &resource,
                true,
                lifecycle.as_mut().expect("write lifecycle"),
            )?;
        }
        if publish
            && let Some(_receipt) = result
                .mutation_receipt
                .as_ref()
                .filter(|receipt| !receipt.is_empty())
        {
            if let Some(candidate) = candidate_bindings {
                // The write visibility lock still covers this swap, so an
                // older request can never overwrite a newer publication.
                *self
                    .semantic_storage_bindings
                    .lock()
                    .expect("semantic storage binding lock poisoned") = Some(candidate.clone());
            }
            if let Some(migration) = &mut legacy_migration {
                migration.commit();
            }
        }
        if is_write
            && result
                .side_effects
                .as_ref()
                .is_some_and(|effects| effects != &graphforge_exec::SideEffects::default())
        {
            self.notice_provider_embedding_mutation();
        }
        Ok(result)
    }

    /// Execute a read-only openCypher query and return a lazy stream of its
    /// result batches (the streaming counterpart of [`execute`](Self::execute)).
    ///
    /// Like `execute`, each batch exposes UUID identity (never internal surrogate
    /// scan keys) while preserving legal user aliases named `node_id`/`edge_id`
    /// (#703), and the stream's schema carries query metadata. `CREATE`/`MERGE`
    /// are not supported on the streaming path — use
    /// [`execute`](Self::execute) for writes.
    ///
    /// # Errors
    /// As [`execute`](Self::execute); additionally [`GfError::Validation`] if the
    /// query is a write.
    pub fn execute_stream(
        &self,
        cypher: &str,
    ) -> Result<graphforge_exec::SendableRecordBatchStream, GfError> {
        self.execute_stream_with_params(cypher, &HashMap::new())
    }

    /// Streaming variant of [`execute_with_params`](Self::execute_with_params).
    ///
    /// # Errors
    /// As [`execute_stream`](Self::execute_stream).
    pub fn execute_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<graphforge_exec::SendableRecordBatchStream, GfError> {
        self.execute_stream_with_workspace(cypher, params)
            .map(|(stream, _)| stream)
    }

    fn execute_stream_with_workspace(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<(graphforge_exec::SendableRecordBatchStream, GraphWorkspace), GfError> {
        use graphforge_exec::ExecutionSession;

        let admission = self.admit_heavy_query_owned()?;
        let _read_visibility = self.graph_visibility.read()?;
        let workspace = self.workspace_for_session();
        let composition = self
            .default_composition_snapshot()
            .map(|context| self.bind_generation_storage(&context))
            .transpose()?;
        if cypher.trim().is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from)?;
        // See `execute_with_params`: a comment-only query strips to zero clauses.
        if ast.clauses.is_empty() {
            return Err(GfError::Validation("empty query".into()));
        }
        validate_typed_parameter_binding(
            &ast,
            params,
            self.ontology.clone(),
            &self.runtime_catalog,
            self.ontology_mode,
            self.procedure_snapshot(),
            composition
                .as_ref()
                .map(|(context, _, _)| Arc::clone(context)),
        )?;
        let plan = {
            let mut binder = Binder::new(
                self.ontology.clone(),
                self.runtime_catalog.clone(),
                self.ontology_mode,
            )
            .with_procedures(self.procedure_snapshot());
            if let Some((context, _, _)) = &composition {
                binder = binder.with_composition(Arc::clone(context));
            }
            binder
                .bind(&ast)
                .map_err(|errs| bind_errors_to_gferror(&errs))?
        };
        validate_call_params(&plan, params)?;
        validate_stream_read_only(&plan)?;

        // Pin every generation-coupled session participant while publication is
        // excluded. `install_property_generation` replaces the authenticated
        // property inventory and ordinal identity authority as one publication
        // transition; opening them without this guard could otherwise combine
        // participants from adjacent generations.
        if composition
            .as_ref()
            .is_some_and(|(_, _, moves)| !moves.is_empty())
        {
            return Err(GfError::Validation(
                "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: run a publishing write to migrate the unambiguous legacy generation".into(),
            ));
        }
        let catalog = self.open_query_catalog(
            &self.runtime_catalog,
            composition.as_ref().map(|(_, candidate, _)| candidate),
        )?;
        let execution_mode = composition
            .as_ref()
            .map_or(self.ontology_mode, |(context, _, _)| {
                Self::composition_execution_mode(context)
            });
        let adjacency_provider = if execution_mode == self.ontology_mode {
            self.adjacency_provider_for_session()
        } else {
            Arc::new(adjacency_provider_for_graph(
                workspace.path(),
                execution_mode,
                self.property_inventory_for_session(),
            )?)
        };
        let session = ExecutionSession::new_with_target_provider_resources_and_identity(
            catalog,
            self.ontology.clone(),
            workspace.path().to_path_buf(),
            execution_mode,
            adjacency_provider,
            Some(Arc::clone(&self.ordinal_identities)),
            &self.session_resource_config(),
        )?;
        let session = if self.read_only {
            session.restrict_to_reads()
        } else {
            session
        };

        // Build the stream on the instance's long-lived runtime so the tasks it
        // spawns (repartition/coalesce) outlive this call — they are dropped
        // only when the `GraphForge` is. `block_on` drives the construction
        // inside that runtime's context.
        let stream = self.block_on(async { session.execute_plan_stream(&plan, params).await })?;
        // Admission is intentionally released after stream construction: the
        // stream is demand-driven and may outlive this call; holding the slot
        // for the full consumer lifetime would serialize all streaming clients.
        drop(admission);
        Ok((
            self.finish_public_stream(stream, workspace.clone()),
            workspace,
        ))
    }

    fn finish_public_stream(
        &self,
        stream: SendableRecordBatchStream,
        workspace: GraphWorkspace,
    ) -> SendableRecordBatchStream {
        Box::pin(WorkspacePinnedStream {
            stream: self.graph_visibility.health.guard_stream(shape_stream(
                stream,
                self.ontology_mode,
                self.ontology.as_ref(),
            )),
            _workspace: workspace,
        })
    }

    /// Streaming query plus a [`RuntimeGuard`] that keeps the instance's
    /// runtime **and** on-disk graph workspace alive for as long as the returned
    /// stream is held — for bindings that detach the stream into a foreign,
    /// lazily-consumed reader (e.g. a `pyarrow.RecordBatchReader`, #587).
    ///
    /// A bare runtime `Handle` does not keep the runtime alive, and streaming
    /// Parquet scans (#339) read fragment paths during consumer pull — so the
    /// guard also pins the private workspace (and in-memory project tempdir)
    /// that back those paths. Resources are released only once both this
    /// `GraphForge` and every outstanding guard drop.
    ///
    /// Returns the (shaped) stream, its schema (advertised up front so a reader
    /// can expose `schema` before the first batch), and the guard.
    ///
    /// # Errors
    /// As [`execute_stream_with_params`](Self::execute_stream_with_params).
    pub fn execute_stream_owned(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
    ) -> Result<
        (
            graphforge_exec::SendableRecordBatchStream,
            SchemaRef,
            RuntimeGuard,
        ),
        GfError,
    > {
        let (stream, workspace) = self.execute_stream_with_workspace(cypher, params)?;
        let schema = stream.schema();
        Ok((
            stream,
            schema,
            RuntimeGuard {
                runtime: Arc::clone(&self.runtime),
                workspace,
                tempdir: self.tempdir.clone(),
            },
        ))
    }

    /// Execute `cypher` and write the result to a Parquet file at `path`.
    ///
    /// This compatibility wrapper uses the bounded streaming sink with default
    /// limits. The write is atomic: a sibling temporary file is published only
    /// after execution, writer finalization, and file sync all succeed.
    ///
    /// # Errors
    /// Propagates any [`execute`](Self::execute) error, or [`GfError::Storage`]
    /// if the file cannot be created, written, or persisted.
    pub fn execute_to_parquet(&self, cypher: &str, path: &str) -> Result<(), GfError> {
        self.execute_to_parquet_with_params(cypher, &HashMap::new(), path)
    }

    /// Params-aware variant of [`execute_to_parquet`](Self::execute_to_parquet):
    /// run `cypher` with `$name` bindings and write the result to `path`.
    ///
    /// # Errors
    /// As [`execute_to_parquet`](Self::execute_to_parquet).
    pub fn execute_to_parquet_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
    ) -> Result<(), GfError> {
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::Parquet,
            &ResultSinkOptions::default(),
            None,
        )
        .map(|_| ())
    }

    /// Stream a query into an atomic Parquet result with explicit limits and
    /// optional cooperative cancellation.
    pub fn execute_to_parquet_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::Parquet,
            options,
            cancellation,
        )
    }

    /// Stream a query into an atomic Arrow IPC stream file with explicit limits
    /// and optional cooperative cancellation.
    pub fn execute_to_arrow_ipc_stream_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        self.execute_to_result_sink_with_params(
            cypher,
            params,
            path,
            ResultSinkFormat::ArrowIpc,
            options,
            cancellation,
        )
    }

    pub(super) fn execute_to_result_sink_with_params(
        &self,
        cypher: &str,
        params: &HashMap<String, IrLiteral>,
        path: &str,
        format: ResultSinkFormat,
        options: &ResultSinkOptions,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ResultSinkReceipt, GfError> {
        cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
        let stream = self.execute_stream_with_params(cypher, params)?;
        let schema = stream.schema();
        let result = self.block_on(async {
            graphforge_io::sink_record_batch_stream_observed(
                stream,
                schema,
                std::path::Path::new(path),
                format,
                options,
                || cancellation.is_some_and(CancellationToken::is_cancelled),
                |path, file| {
                    let Some(allocation) = &self.allocation_operation else {
                        return Ok(());
                    };
                    let path = if path.is_absolute() {
                        path.to_path_buf()
                    } else {
                        std::env::current_dir()
                            .map_err(|error| error.to_string())?
                            .join(path)
                    };
                    match file {
                        Some(file) => allocation.replace_file_at(&path, file),
                        None => allocation.remove_file_at(&path),
                    }
                    .map_err(|error| error.to_string())
                },
            )
            .await
            .map_err(|error| {
                if error.phase == "cancelled" {
                    GfError::Api {
                        code: ApiErrorCode::Cancelled,
                        message: error.to_string(),
                    }
                } else {
                    GfError::Storage(error.to_string())
                }
            })
        })?;
        Ok(result)
    }
}

fn materialize_row_count_params(
    plan: &GraphPlan,
    params: &HashMap<String, IrLiteral>,
) -> Result<GraphPlan, GfError> {
    let mut plan = plan.clone();
    if plan_contains_aggregate(&plan) {
        plan.exprs.substitute_parameters(params);
    }
    materialize_row_count_ops(&mut plan.ops, params)?;
    Ok(plan)
}

fn plan_contains_aggregate(plan: &GraphPlan) -> bool {
    plan.ops.iter().any(|op| match op {
        GraphOp::Aggregate { .. } => true,
        GraphOp::Optional { child }
        | GraphOp::Exists { child, .. }
        | GraphOp::PatternComprehension { child, .. }
        | GraphOp::ListElementPatternComprehension { child, .. } => plan_contains_aggregate(child),
        GraphOp::Union { inputs, .. } => inputs.iter().any(plan_contains_aggregate),
        _ => false,
    })
}

fn materialize_row_count_ops(
    ops: &mut [GraphOp],
    params: &HashMap<String, IrLiteral>,
) -> Result<(), GfError> {
    for op in ops {
        match op {
            GraphOp::SkipParam { name } => {
                let count = row_count_param_value("SKIP", name, params)?;
                *op = GraphOp::Skip { count };
            }
            GraphOp::LimitParam { name } => {
                let count = row_count_param_value("LIMIT", name, params)?;
                *op = GraphOp::Limit { count };
            }
            GraphOp::Optional { child }
            | GraphOp::Exists { child, .. }
            | GraphOp::PatternComprehension { child, .. }
            | GraphOp::ListElementPatternComprehension { child, .. } => {
                materialize_row_count_ops(&mut child.ops, params)?;
            }
            GraphOp::Union { inputs, .. } => {
                for input in inputs {
                    materialize_row_count_ops(&mut input.ops, params)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn row_count_param_value(
    keyword: &str,
    name: &str,
    params: &HashMap<String, IrLiteral>,
) -> Result<u64, GfError> {
    match params.get(name) {
        Some(IrLiteral::Int(n)) => u64::try_from(*n).map_err(|_| {
            GfError::Execution(format!(
                "{keyword} parameter `${name}` must be a non-negative integer"
            ))
        }),
        Some(_) => Err(GfError::Execution(format!(
            "{keyword} parameter `${name}` must be an integer"
        ))),
        None => Err(GfError::Execution(format!(
            "missing query parameter `${name}` for {keyword}"
        ))),
    }
}

/// Remap planner-surface failures that the public API classifies as execution
/// errors: unbound query parameters and arithmetic/type coercion mismatches.
fn publicize_query_error(err: GfError) -> GfError {
    match err {
        // Preserve the established foreign-DataFusion coercion/placeholder
        // classification without discarding the lowering diagnostic (#1018).
        GfError::Lowering(error @ LoweringError::UnsupportedExpr(_))
            if is_public_execution_plan_failure(&error.to_string()) =>
        {
            GfError::LoweringExecution(error)
        }
        GfError::Plan(msg) if is_public_execution_plan_failure(&msg) => {
            let msg = msg
                .strip_prefix("Execution error: ")
                .unwrap_or(&msg)
                .to_owned();
            GfError::Execution(msg)
        }
        other => other,
    }
}

fn is_public_execution_plan_failure(msg: &str) -> bool {
    msg.contains("Placeholder '")
        || msg.contains("Placeholder \"$")
        || msg.contains("placeholder with name $")
        || msg.contains("No value found for placeholder")
        || msg.contains("was not provided a value for execution")
        || msg.contains("Cannot coerce")
}

/// Collapse a binder's `Vec<BindError>` into a span-rich [`GfError::Bind`]
/// (#606). The binder collects every problem before returning, so `msg` lists
/// them all; `span` carries the *first* error's location so callers (and the
/// Python/Node bindings) can point at the offending token.
fn bind_errors_to_gferror(errs: &[BindError]) -> GfError {
    GfError::from_bind_errors(errs)
}

fn validate_stream_read_only(plan: &GraphPlan) -> Result<(), GfError> {
    if plan.ops.iter().any(|op| {
        matches!(
            op,
            GraphOp::Create { .. }
                | GraphOp::Merge { .. }
                | GraphOp::Delete { .. }
                | GraphOp::Set { .. }
                | GraphOp::Remove { .. }
        )
    }) {
        return Err(GfError::Validation(
            "execute_stream does not support writes; \
                 use execute for CREATE/MERGE/DELETE/SET/REMOVE"
                .into(),
        ));
    }
    Ok(())
}

fn validate_typed_parameter_binding(
    query: &graphforge_cypher::AstQuery,
    params: &HashMap<String, IrLiteral>,
    ontology: Option<OntologyHandle>,
    runtime_catalog: &Arc<Mutex<RuntimeCatalog>>,
    mode: OntologyMode,
    procedures: Arc<ProcedureRegistry>,
    composition: Option<Arc<CompositionBindingContext>>,
) -> Result<(), GfError> {
    if !params.values().any(ir_literal_contains_uuid) {
        return Ok(());
    }
    let catalog = Arc::new(Mutex::new(
        runtime_catalog
            .lock()
            .expect("runtime catalog poisoned")
            .clone(),
    ));
    let mut binder = Binder::new(ontology, catalog, mode)
        .with_procedures(procedures)
        .with_parameter_literals(params);
    if let Some(context) = composition {
        binder = binder.with_composition(context);
    }
    binder
        .bind(query)
        .map(|_| ())
        .map_err(|errors| bind_errors_to_gferror(&errors))
}

fn ir_literal_contains_uuid(value: &IrLiteral) -> bool {
    match value {
        IrLiteral::Uuid(_) => true,
        IrLiteral::List(items) => items.iter().any(ir_literal_contains_uuid),
        IrLiteral::Map(entries) => entries
            .iter()
            .any(|(_, value)| ir_literal_contains_uuid(value)),
        _ => false,
    }
}

fn validate_call_params(
    plan: &GraphPlan,
    params: &HashMap<String, IrLiteral>,
) -> Result<(), GfError> {
    for op in &plan.ops {
        if let GraphOp::Call { args, .. } = op {
            for arg in args {
                if let IrExpr::Parameter(name) = plan.exprs.get(*arg)
                    && !params.contains_key(name)
                {
                    return Err(GfError::Bind {
                        diagnostics: Vec::new(),
                        msg: format!("MissingParameter: no value supplied for `${name}`"),
                        span: Span::default(),
                    });
                }
            }
        }
    }
    Ok(())
}

// Drop the lazy reader (and its retained descriptors) before the final
// workspace owner. Compaction may rotate the facade while this stream lives.
struct WorkspacePinnedStream {
    stream: SendableRecordBatchStream,
    _workspace: GraphWorkspace,
}

impl futures::Stream for WorkspacePinnedStream {
    type Item = datafusion::error::Result<arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}

impl datafusion::physical_plan::RecordBatchStream for WorkspacePinnedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.stream.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_query_preflight_errors_are_exact_and_side_effect_free() {
        let graph = GraphForge::new(None).unwrap();
        let error =
            |result: Result<graphforge_exec::SendableRecordBatchStream, GfError>| match result {
                Ok(_) => panic!("expected streaming query to fail"),
                Err(error) => error,
            };

        let empty = error(graph.execute_stream("   "));
        assert_eq!(empty.code(), "GF_VALIDATION");
        assert!(empty.to_string().contains("empty query"));

        let comment = error(graph.execute_stream("// comment only"));
        assert_eq!(comment.code(), "GF_VALIDATION");
        assert!(comment.to_string().contains("empty query"));

        let parse = error(graph.execute_stream("MATCH ("));
        assert_eq!(parse.code(), "GF_PARSE");

        let missing = error(
            graph.execute_stream_with_params("MATCH (n) RETURN n SKIP $missing", &HashMap::new()),
        );
        assert_eq!(missing.code(), "GF_PLAN");
        assert_eq!(
            missing.to_string(),
            "plan error: unsupported expression: operator not yet lowered (deferred to #577+): \
             SkipParam { name: \"missing\" }"
        );
    }

    #[test]
    fn row_count_boundaries_match_public_error_domains() {
        let mut params = HashMap::new();
        params.insert("count".to_owned(), IrLiteral::Int(7));
        assert_eq!(row_count_param_value("LIMIT", "count", &params).unwrap(), 7);

        params.insert("count".to_owned(), IrLiteral::Int(-1));
        let negative = row_count_param_value("LIMIT", "count", &params).unwrap_err();
        assert_eq!(negative.code(), "GF_EXECUTION");
        assert_eq!(
            negative.to_string(),
            "execution error: LIMIT parameter `$count` must be a non-negative integer"
        );

        params.insert("count".to_owned(), IrLiteral::Str("7".to_owned()));
        let wrong_type = row_count_param_value("SKIP", "count", &params).unwrap_err();
        assert_eq!(wrong_type.code(), "GF_EXECUTION");
        assert_eq!(
            wrong_type.to_string(),
            "execution error: SKIP parameter `$count` must be an integer"
        );

        let missing = row_count_param_value("LIMIT", "absent", &params).unwrap_err();
        assert_eq!(missing.code(), "GF_EXECUTION");
        assert_eq!(
            missing.to_string(),
            "execution error: missing query parameter `$absent` for LIMIT"
        );
    }
}
