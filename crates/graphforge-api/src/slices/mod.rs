//! Bounded native selection; membership is distinct from research ownership.
pub(crate) mod branch;
mod engine;
mod frozen;
mod graph;
mod ledger;
mod model;
mod output;

use crate::{CancellationToken, ExecutionResult, GraphForge, PageRequest};
use graphforge_core::{ApiErrorCode, GfError};
pub use model::*;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Object {
    kind: String,
    uuid: Uuid,
}
impl Object {
    fn new(kind: &str, uuid: Uuid) -> Self {
        Self {
            kind: kind.into(),
            uuid,
        }
    }
}
#[derive(Clone, Debug)]
struct Inclusion {
    reason: String,
    root: Uuid,
    predecessor: Option<Uuid>,
    via_edge: Option<Uuid>,
    depth: u32,
}
impl Inclusion {
    fn direct(id: Uuid, reason: &str) -> Self {
        Self {
            reason: reason.into(),
            root: id,
            predecessor: None,
            via_edge: None,
            depth: 0,
        }
    }
}
#[derive(Default)]
struct Selection {
    active: BTreeMap<Object, Inclusion>,
    required: BTreeMap<Object, Inclusion>,
    boundary: BTreeMap<Object, Inclusion>,
    labels: BTreeMap<Object, Vec<String>>,
}

fn error(code: ApiErrorCode, message: &str) -> GfError {
    GfError::Api {
        code,
        message: message.into(),
    }
}
fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}
fn limit() -> GfError {
    error(
        ApiErrorCode::ResourceLimit,
        "Slice resource bound exceeded; narrow selection or raise its explicit limits",
    )
}
fn unavailable() -> GfError {
    error(
        ApiErrorCode::ResultNotRetained,
        "Slice object is unavailable in the selected historical authority; select a separately retained source Version explicitly",
    )
}
fn checkpoint(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    cancellation.map_or(Ok(()), CancellationToken::checkpoint)
}

impl GraphForge {
    /// Evaluate a dynamic selection against one pinned source. Continuations reject
    /// changed source or selection; required context never expands active membership.
    pub fn preview_slice(
        &self,
        request: &SliceRequest,
        kind: SlicePageKind,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError> {
        graph::validate(request)?;
        checkpoint(page.cancellation.as_ref())?;
        let (view, snapshot) = graph::open(self, request)?;
        let mut selection = engine::evaluate(&view, request, page.cancellation.as_ref())?;
        genealogy(self, request, &mut selection)?;
        output::page(&selection, request, snapshot, kind, page, None)
    }
}

fn genealogy(
    owner: &GraphForge,
    request: &SliceRequest,
    selection: &mut Selection,
) -> Result<(), GfError> {
    if let SliceSource::Version { version_uuid } = request.source
        && let Some(source) = owner.research_version(version_uuid)?.content.source_version
    {
        selection.boundary.insert(
            Object::new("source_version", source),
            Inclusion::direct(version_uuid, "outside_genealogy_not_retained_by_membership"),
        );
        if selection.boundary.len() > request.limits.boundary_references as usize {
            return Err(limit());
        }
    }
    Ok(())
}

/// Shared bounded native read path for Branch preparation, without collecting a
/// complete query result before cancellation and allocation accounting.
pub(crate) fn stream_branch(
    view: &GraphForge,
    query: &str,
    cancellation: &CancellationToken,
    consume: impl FnMut(&arrow::record_batch::RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let request = SliceRequest {
        request_uuid: Uuid::now_v7(),
        source: SliceSource::Current,
        selector: SliceSelector::Direct {
            members: SliceMembers::default(),
        },
        include: SliceMembers::default(),
        exclude: SliceMembers::default(),
        limits: SliceLimits::default(),
    };
    let mut budget = graph::Budget::new(&request, Some(cancellation));
    graph::stream(
        view,
        query,
        &std::collections::HashMap::new(),
        &mut budget,
        consume,
    )
}
