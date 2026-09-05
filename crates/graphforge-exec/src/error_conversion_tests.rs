//! Real DataFusion error propagation must retain GraphForge's public identity.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::Diagnostic;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Volatility, col, create_udf};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode, Span};
use graphforge_ir::{GraphPlan, RuntimeCatalog};
use graphforge_storage::GraphCatalog;
use tempfile::TempDir;

use super::ExecutionSession;

#[test]
fn datafusion_error_source_preserves_binder_span_through_shared_context() {
    let original = GfError::Bind {
        msg: "undeclared variable".into(),
        span: Span { start: 7, end: 14 },
    };
    let wrapped = Arc::new(DataFusionError::Context(
        "physical operator".into(),
        Box::new(DataFusionError::External(Box::new(original.clone()))),
    ));
    // Two consumers preclude using exclusive Arc ownership to recover the error.
    for _ in 0..2 {
        let recovered = GfError::from_plan_error(DataFusionError::Shared(Arc::clone(&wrapped)));
        assert_eq!(recovered.code(), "GF_PARSE");
        assert_eq!(recovered.to_string(), original.to_string());
        let GfError::Bind { msg, span } = recovered else {
            panic!("binder variant was lost");
        };
        assert_eq!(msg, "undeclared variable");
        assert_eq!(span, Span { start: 7, end: 14 });
    }
}

#[test]
fn datafusion_error_source_preserves_typed_resource_failure() {
    let original = resource_failure();
    let expected = original.to_string();
    let recovered = GfError::from_execution_error(DataFusionError::External(Box::new(original)));
    assert_eq!(recovered.code(), "GF_RESOURCE_LIMIT");
    assert_eq!(recovered.to_string(), expected);
    assert!(matches!(
        recovered,
        GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            ..
        }
    ));
}

#[test]
fn datafusion_error_source_does_not_classify_diagnostic_text() {
    let foreign = DataFusionError::Execution("GF_RESOURCE_LIMIT is just text".into());
    let expected = foreign.to_string();
    let recovered = GfError::from_execution_error(foreign);
    assert_eq!(recovered.code(), "GF_EXECUTION");
    assert!(matches!(recovered, GfError::Execution(ref message) if message == &expected));
    let foreign = DataFusionError::Plan("missing column".into());
    let expected = foreign.to_string();
    let recovered = GfError::from_plan_error(foreign);
    assert!(matches!(recovered, GfError::Plan(ref message) if message == &expected));
}

fn resource_failure() -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: "bounded operator exhausted its budget".into(),
    }
}

async fn failing_dataframe() -> datafusion::dataframe::DataFrame {
    let context = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "input",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let function = create_udf(
        "graphforge_structured_failure",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(|_| Err(super::to_df_err(resource_failure()))),
    );
    context
        .read_batch(batch)
        .unwrap()
        .select(vec![function.call(vec![col("input")])])
        .unwrap()
}

// Inject only the physical planner result. The actual ExecutionSession entry
// points still perform lowering, planning, collecting and stream wrapping, so
// reverting a production conversion makes these tests fail.
#[derive(Debug)]
enum BoundaryPlanner {
    Physical(Arc<dyn ExecutionPlan>),
    Failure(GfError),
}

#[async_trait]
impl QueryPlanner for BoundaryPlanner {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match self {
            Self::Physical(physical) => Ok(Arc::clone(physical)),
            Self::Failure(error) => Err(DataFusionError::Context(
                "test physical planner".into(),
                Box::new(super::to_df_err(error.clone())),
            )),
        }
    }
}

fn boundary_session(planner: BoundaryPlanner) -> (TempDir, ExecutionSession) {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let mut session = ExecutionSession::new(catalog, None).unwrap();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(planner))
        .build();
    session.ctx = SessionContext::new_with_state(state);
    (dir, session)
}

fn assert_resource_failure(error: GfError) {
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert_eq!(error.to_string(), resource_failure().to_string());
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            ..
        }
    ));
}

#[tokio::test]
async fn datafusion_error_source_survives_session_collect() {
    let physical = failing_dataframe()
        .await
        .create_physical_plan()
        .await
        .unwrap();
    let (_dir, session) = boundary_session(BoundaryPlanner::Physical(physical));
    let plan = GraphPlan::builder("openCypher").build();
    let failure = session
        .execute_plan(&plan)
        .await
        .expect_err("physical UDF must fail through the production collecting boundary");
    assert_resource_failure(failure);
}

#[tokio::test]
async fn datafusion_error_source_survives_session_stream_poll() {
    let physical = failing_dataframe()
        .await
        .create_physical_plan()
        .await
        .unwrap();
    let (_dir, session) = boundary_session(BoundaryPlanner::Physical(physical));
    let plan = GraphPlan::builder("openCypher").build();
    let mut stream = session
        .execute_plan_stream(&plan, &HashMap::new())
        .await
        .unwrap();
    let failure = stream
        .next()
        .await
        .expect("physical failure batch")
        .expect_err("physical UDF must fail");
    // The production session stream is DataFusion-typed; consumers recover the
    // original source after QueryEvidenceStream has finalized its evidence.
    assert_resource_failure(GfError::from_execution_error(failure));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn datafusion_error_source_survives_session_planning_for_collect_and_stream() {
    let original = GfError::Bind {
        msg: "physical planner preserved source".into(),
        span: Span { start: 3, end: 9 },
    };
    let (_dir, session) = boundary_session(BoundaryPlanner::Failure(original.clone()));
    let plan = GraphPlan::builder("openCypher").build();
    let collecting = session.execute_plan(&plan).await.unwrap_err();
    let streaming = match session.execute_plan_stream(&plan, &HashMap::new()).await {
        Err(error) => error,
        Ok(_) => panic!("physical planning must fail before opening a stream"),
    };
    for recovered in [collecting, streaming] {
        assert_eq!(recovered.code(), "GF_PARSE");
        assert_eq!(recovered.to_string(), original.to_string());
        assert!(matches!(
            recovered,
            GfError::Bind {
                span: Span { start: 3, end: 9 },
                ..
            }
        ));
    }
}

// A physical operator may fail when execute() opens its stream, before polling.
#[derive(Debug)]
struct StreamStartFailure {
    properties: Arc<PlanProperties>,
}

impl DisplayAs for StreamStartFailure {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StreamStartFailure")
    }
}

impl ExecutionPlan for StreamStartFailure {
    fn name(&self) -> &str {
        "StreamStartFailure"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Err(super::to_df_err(resource_failure()))
    }
}

#[tokio::test]
async fn datafusion_error_source_survives_session_stream_initialization() {
    let empty = datafusion::physical_plan::empty::EmptyExec::new(Arc::new(Schema::empty()));
    let physical = Arc::new(StreamStartFailure {
        properties: Arc::clone(empty.properties()),
    });
    let (_dir, session) = boundary_session(BoundaryPlanner::Physical(physical));
    let plan = GraphPlan::builder("openCypher").build();
    let failure = match session.execute_plan_stream(&plan, &HashMap::new()).await {
        Err(error) => error,
        Ok(_) => panic!("opening the physical stream must fail"),
    };
    assert_resource_failure(failure);
}

#[test]
fn datafusion_error_source_preserves_project_code_through_arrow_and_diagnostic() {
    let original = GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: "selected graph authority failed authentication".into(),
    };
    let wrapped = DataFusionError::ArrowError(
        Box::new(arrow::error::ArrowError::ExternalError(Box::new(
            original.clone(),
        ))),
        None,
    )
    .with_diagnostic(Diagnostic::new_error("physical read failed", None));
    let recovered = GfError::from_execution_error(wrapped);
    assert_eq!(recovered.code(), "GF_PROJECT_CORRUPT");
    assert_eq!(recovered.to_string(), original.to_string());
    assert!(matches!(
        recovered,
        GfError::Project {
            code: ProjectErrorCode::ProjectCorrupt,
            ..
        }
    ));
}

#[test]
fn datafusion_collection_preserves_only_the_primary_source() {
    let primary = DataFusionError::Collection(vec![
        super::to_df_err(resource_failure()),
        DataFusionError::Execution("secondary".into()),
    ]);
    assert_resource_failure(GfError::from_execution_error(primary));

    // DataFusion exposes the first collection member through Error::source.
    // A secondary GraphForge diagnostic must not replace a foreign primary.
    let foreign_primary = DataFusionError::Collection(vec![
        DataFusionError::Plan("primary failure".into()),
        super::to_df_err(resource_failure()),
    ]);
    let expected = foreign_primary.to_string();
    let recovered = GfError::from_plan_error(foreign_primary);
    assert!(matches!(recovered, GfError::Plan(message) if message == expected));
}
