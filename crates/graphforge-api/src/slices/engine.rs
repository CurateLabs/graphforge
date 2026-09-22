//! Canonical membership and one bounded predecessor explanation per object.
use super::{
    CancellationToken, GfError, GraphForge, Inclusion, Object, Selection, SliceDirection,
    SliceLimits, SliceMembers, SliceRequest, SliceSelector, SliceSource, Uuid, checkpoint,
    graph::{self, Budget, Topology},
    invalid, limit, unavailable,
};
use crate::IrLiteral;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

pub(super) fn objects(members: &SliceMembers) -> impl Iterator<Item = Object> + '_ {
    [
        ("node", &members.nodes),
        ("edge", &members.edges),
        ("source", &members.sources),
        ("artifact", &members.artifacts),
        ("assertion", &members.assertions),
    ]
    .into_iter()
    .flat_map(|(kind, ids)| ids.iter().map(move |id| Object::new(kind, *id)))
}
fn selected(
    selection: &mut Selection,
    object: Object,
    reason: Inclusion,
    limits: &SliceLimits,
) -> Result<(), GfError> {
    selection.active.entry(object).or_insert(reason);
    if selection.active.len() > limits.selected_objects as usize {
        return Err(limit());
    }
    Ok(())
}
pub(super) fn evaluate(
    view: &GraphForge,
    request: &SliceRequest,
    cancellation: Option<&CancellationToken>,
) -> Result<Selection, GfError> {
    let mut budget = Budget::new(request, cancellation);
    let topology = graph::topology(view, &mut budget)?;
    let mut selection = Selection::default();
    match &request.selector {
        SliceSelector::Direct { members } => {
            for object in objects(members) {
                let why = Inclusion::direct(object.uuid, "direct");
                selected(&mut selection, object, why, &request.limits)?;
            }
        }
        SliceSelector::Traverse {
            seeds,
            direction,
            max_depth,
            relationship_types,
        } => {
            traverse(
                &topology,
                seeds,
                *direction,
                *max_depth,
                relationship_types,
                &mut selection,
                &mut budget,
            )?;
        }
        SliceSelector::Query { query } => select_query(
            view,
            query,
            &HashMap::new(),
            "query",
            &mut selection,
            &mut budget,
        )?,
        SliceSelector::Filter {
            label,
            property,
            equals,
        } => {
            let query = format!(
                "MATCH (n:{}) WHERE n.{} = $slice_value RETURN n.node_uuid AS node_uuid",
                identifier(label)?,
                identifier(property)?
            );
            let params = HashMap::from([("slice_value".into(), scalar(equals)?)]);
            select_query(view, &query, &params, "filter", &mut selection, &mut budget)?;
        }
        SliceSelector::Search {
            label,
            text,
            limit: hits,
        } => {
            search(
                view,
                request,
                label,
                text,
                *hits,
                &mut selection,
                &mut budget,
            )?;
        }
    }
    for object in objects(&request.include) {
        checkpoint(cancellation)?;
        let reason = Inclusion::direct(object.uuid, "explicit_include");
        selected(&mut selection, object, reason, &request.limits)?;
    }
    for object in objects(&request.exclude) {
        selection.active.remove(&object);
    }
    for object in selection.active.keys() {
        let labels = match object.kind.as_str() {
            "node" => topology
                .nodes
                .get(&object.uuid)
                .cloned()
                .ok_or_else(unavailable)?,
            "edge" => vec![
                topology
                    .edges
                    .get(&object.uuid)
                    .ok_or_else(unavailable)?
                    .label
                    .clone(),
            ],
            _ => vec![object.kind.clone()],
        };
        selection.labels.insert(object.clone(), labels);
    }
    let context_uuid = match request.source {
        SliceSource::Current => view.generation_for_read()?.generation_uuid(),
        SliceSource::Version { version_uuid } => version_uuid,
    };
    super::ledger::dependencies(view, &topology, &mut selection, &mut budget, context_uuid)?;
    boundary(&topology, &mut selection, &mut budget)?;
    Ok(selection)
}
fn select_query(
    view: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
    reason: &str,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
) -> Result<(), GfError> {
    let limits = budget.limits.clone();
    graph::stream(view, query, params, budget, |batch| {
        let names: Vec<_> = ["node_uuid", "edge_uuid"]
            .into_iter()
            .filter(|name| batch.column_by_name(name).is_some())
            .collect();
        if names.is_empty() {
            return Err(invalid("Slice query must return node_uuid or edge_uuid"));
        }
        for row in 0..batch.num_rows() {
            for name in &names {
                let id = graph::uuid(batch, name, row)?;
                selected(
                    selection,
                    Object::new(if *name == "node_uuid" { "node" } else { "edge" }, id),
                    Inclusion::direct(id, reason),
                    &limits,
                )?;
            }
        }
        Ok(())
    })
}
fn identifier(value: &str) -> Result<String, GfError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(invalid("Slice filter identifier is empty or oversized"));
    }
    Ok(format!("`{}`", value.replace('`', "``")))
}
fn scalar(value: &serde_json::Value) -> Result<IrLiteral, GfError> {
    Ok(match value {
        serde_json::Value::Null => IrLiteral::Null,
        serde_json::Value::Bool(v) => IrLiteral::Bool(*v),
        serde_json::Value::String(v) => IrLiteral::Str(v.clone()),
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                IrLiteral::Int(v)
            } else {
                IrLiteral::Float(v.as_f64().ok_or_else(|| invalid("invalid Slice scalar"))?)
            }
        }
        _ => return Err(invalid("Slice filter requires a scalar")),
    })
}
#[allow(clippy::too_many_arguments)]
fn traverse(
    topology: &Topology,
    seeds: &BTreeSet<Uuid>,
    direction: SliceDirection,
    max_depth: u32,
    types: &BTreeSet<String>,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
) -> Result<(), GfError> {
    if max_depth > 64 {
        return Err(invalid("Slice traversal max_depth exceeds 64"));
    }
    let mut adjacency: BTreeMap<Uuid, Vec<(Uuid, Uuid)>> = BTreeMap::new();
    for (id, edge) in &topology.edges {
        checkpoint(budget.cancellation)?;
        if !types.is_empty() && !types.contains(&edge.label) {
            continue;
        }
        budget.charge(0, 256)?;
        if direction != SliceDirection::Incoming {
            adjacency
                .entry(edge.source)
                .or_default()
                .push((*id, edge.target));
        }
        if direction != SliceDirection::Outgoing {
            adjacency
                .entry(edge.target)
                .or_default()
                .push((*id, edge.source));
        }
    }
    let mut queue = VecDeque::new();
    for seed in seeds {
        if !topology.nodes.contains_key(seed) {
            return Err(unavailable());
        }
        selected(
            selection,
            Object::new("node", *seed),
            Inclusion::direct(*seed, "traversal_seed"),
            budget.limits,
        )?;
        queue.push_back((*seed, *seed, 0));
    }
    while let Some((node, root, depth)) = queue.pop_front() {
        checkpoint(budget.cancellation)?;
        if depth == max_depth {
            continue;
        }
        for (edge, target) in adjacency.get(&node).into_iter().flatten() {
            let why = Inclusion {
                reason: "traversal".into(),
                root,
                predecessor: Some(node),
                via_edge: Some(*edge),
                depth: depth + 1,
            };
            selected(
                selection,
                Object::new("edge", *edge),
                why.clone(),
                budget.limits,
            )?;
            let object = Object::new("node", *target);
            if !selection.active.contains_key(&object) {
                selected(selection, object, why, budget.limits)?;
                queue.push_back((*target, root, depth + 1));
            }
        }
    }
    Ok(())
}
fn boundary(
    topology: &Topology,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
) -> Result<(), GfError> {
    for (id, edge) in &topology.edges {
        checkpoint(budget.cancellation)?;
        if selection.active.contains_key(&Object::new("edge", *id)) {
            continue;
        }
        let source = selection
            .active
            .contains_key(&Object::new("node", edge.source));
        let target = selection
            .active
            .contains_key(&Object::new("node", edge.target));
        if !source && !target {
            continue;
        }
        let inside = if source { edge.source } else { edge.target };
        let outside = if source { edge.target } else { edge.source };
        let reason = Inclusion {
            reason: "outside_relationship".into(),
            root: inside,
            predecessor: Some(inside),
            via_edge: Some(*id),
            depth: 1,
        };
        selection
            .boundary
            .entry(Object::new("edge", *id))
            .or_insert(reason.clone());
        if !selection.active.contains_key(&Object::new("node", outside)) {
            selection
                .boundary
                .entry(Object::new("node", outside))
                .or_insert(reason);
        }
        budget.charge(0, 512)?;
        if selection.boundary.len() > budget.limits.boundary_references as usize {
            return Err(limit());
        }
    }
    Ok(())
}

fn search(
    view: &GraphForge,
    request: &SliceRequest,
    label: &str,
    text: &str,
    hits: u32,
    selection: &mut Selection,
    budget: &mut Budget<'_>,
) -> Result<(), GfError> {
    let cancellation = budget.cancellation;
    if hits == 0 || hits > request.limits.selected_objects {
        return Err(invalid("Slice search hit limit exceeds selected_objects"));
    }
    checkpoint(cancellation)?;
    let limits = graphforge_search::TextSearchLimits {
        topology_rows: request.limits.scanned_rows as usize,
        property_rows: request.limits.scanned_rows as usize,
        documents: request.limits.scanned_rows as usize,
        source_bytes: request.limits.working_bytes,
        writer_memory_bytes: usize::try_from(request.limits.working_bytes).map_err(|_| limit())?,
        index_bytes: request.limits.working_bytes,
        results: request.limits.selected_objects as usize,
        ..Default::default()
    };
    let observe = || {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            Err(graphforge_storage::SearchArtifactError::Cancelled)
        } else {
            Ok(())
        }
    };
    let workspace = view.workspace_for_session();
    let projection = graphforge_search::project_text_source(
        workspace.path(),
        view.find_label_id(label)?,
        None,
        limits,
        observe,
    )?;
    // Reuse the exact native projection/analyzer/Tantivy implementation,
    // but keep its rebuildable index outside the immutable generation.
    let scratch = tempfile::tempdir().map_err(|e| GfError::Storage(e.to_string()))?;
    let built = graphforge_search::build_text_index(scratch.path(), &projection, limits, observe)?;
    let hits = if matches!(built, graphforge_search::TextIndexBuildOutcome::Empty) {
        Vec::new()
    } else {
        graphforge_search::search_text_index(
            scratch.path(),
            &projection.properties,
            text,
            hits as usize,
            limits,
            observe,
        )?
    };
    budget.charge(hits.len() as u64, hits.len() as u64 * 256)?;
    for hit in hits {
        let id = Uuid::from_bytes(hit.node_uuid);
        selected(
            selection,
            Object::new("node", id),
            Inclusion::direct(id, "search"),
            &request.limits,
        )?;
    }
    Ok(())
}
