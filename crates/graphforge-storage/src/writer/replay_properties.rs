//! Replay property routes and streamed fragment ownership.

use super::Arc;
use super::ArrayRef;
use super::BTreeMap;
use super::BooleanArray;
use super::ColType;
use super::DataType;
use super::EDGE_PROPERTY_UUID_FIELD;
use super::Field;
use super::FixedSizeBinaryArray;
use super::GfError;
use super::HashMap;
use super::IrLiteral;
use super::NODE_PROPERTY_UUID_FIELD;
use super::Path;
use super::PropRow;
use super::RecordBatch;
use super::RewriteBatch;
use super::Schema;
use super::SchemaRef;
use super::build_property_array;
use super::col_type_from_field;
use super::fs;
use super::io_err;
use super::pq_err;
use super::reject_map_property_value;
use super::replay_resource_limit;
use super::replay_writer_properties;
use super::replay_writer_reservation;
use super::size_of;
use super::uuid_field;

#[allow(clippy::too_many_lines)] // Node and edge property paths deliberately share one writer.
pub(super) fn stream_replay_properties(
    target: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    edge: bool,
    route_table: &mut crate::route_component::RouteTable,
) -> Result<(), GfError> {
    let operations = if edge {
        &overlay.edge_properties
    } else {
        &overlay.node_properties
    };
    let kind = if edge {
        crate::PropertyRouteKind::Edge
    } else {
        crate::PropertyRouteKind::Node
    };
    let mut fragment_routes = operations
        .keys()
        .map(|(_, route, _)| route.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let deletes_entity_with_unknown_routes = if edge {
        overlay.edges.values().any(Option::is_none)
    } else {
        overlay.nodes.values().any(Option::is_none)
    };
    if deletes_entity_with_unknown_routes {
        // Entity deletion must remove its property row from every route. Other
        // mutations affect only their explicitly routed property fragments;
        // rewriting all authenticated routes would duplicate unchanged full
        // snapshots on every journal materialization.
        fragment_routes.extend(inventory.routes(kind).map(str::to_owned));
    }
    if fragment_routes.is_empty() {
        return Ok(());
    }
    // Legacy flat baselines are admitted as generation zero fragments and are
    // resolved through the same byte-charged chunk path. There is no second
    // unbounded flat-file replay fallback.
    stream_replay_property_fragments(
        target,
        inventory,
        overlay,
        limits,
        edge,
        fragment_routes,
        route_table,
    )
}

fn stream_replay_property_fragments(
    target: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    edge: bool,
    routes: std::collections::BTreeSet<String>,
    route_table: &mut crate::route_component::RouteTable,
) -> Result<(), GfError> {
    for route in routes {
        stream_replay_property_route_with_table(
            target,
            inventory,
            overlay,
            limits,
            edge,
            &route,
            route_table,
        )?;
    }
    Ok(())
}

fn replay_property_resource_schema(logical: &Schema) -> Schema {
    let mut fields = logical
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.insert(
        1,
        Field::new(
            crate::property_overlay::PROPERTY_TOMBSTONE_FIELD,
            DataType::Boolean,
            false,
        ),
    );
    Schema::new_with_metadata(fields, logical.metadata().clone())
}

fn parse_replay_property_uuids(
    names: &[&str],
) -> Result<std::collections::BTreeSet<[u8; 16]>, GfError> {
    names
        .iter()
        .map(|uuid| {
            uuid::Uuid::parse_str(uuid)
                .map(uuid::Uuid::into_bytes)
                .map_err(pq_err)
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
}

struct ReplayPropertyFragmentWriter {
    extra_metadata_bytes: usize,
    logical_schema: Schema,
    physical_schema: SchemaRef,
    writer: parquet::arrow::ArrowWriter<fs::File>,
}

type ReplayPropertyRows = (
    BTreeMap<[u8; 16], crate::PropertySnapshotRow>,
    Vec<crate::PropertySnapshotRow>,
);

struct ReplayPropertyRouteContext<'a> {
    target: &'a Path,
    inventory: &'a crate::AuthenticatedPropertyInventory,
    overlay: &'a crate::graph_delta_journal::ReplayOverlay,
    operations: &'a ReplayPropertyOperations,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    kind: crate::PropertyRouteKind,
    edge: bool,
    route: &'a str,
    overlay_bytes: usize,
    retained_target_bytes: usize,
    writer_reservation_bytes: usize,
    logical_schema: SchemaRef,
}

impl ReplayPropertyRouteContext<'_> {
    fn fragment_writer<'a>(
        &self,
        fragment: &'a mut Option<ReplayPropertyFragmentWriter>,
    ) -> Result<&'a mut ReplayPropertyFragmentWriter, GfError> {
        if fragment.is_none() {
            *fragment = Some(open_replay_property_fragment(
                self.target,
                self.kind,
                self.edge,
                self.route,
                self.limits.max_batch_rows,
                &self.logical_schema,
            )?);
        }
        Ok(fragment
            .as_mut()
            .expect("property replay fragment initialized"))
    }

    fn process_chunk(
        &self,
        names: &[&str],
        fragment: &mut Option<ReplayPropertyFragmentWriter>,
    ) -> Result<(), GfError> {
        let retained_writer = fragment.as_ref().map_or(0, |writer| {
            self.writer_reservation_bytes
                .saturating_add(writer.extra_metadata_bytes)
        });
        let (before, rows) = self.property_rows(names, retained_writer)?;
        drop(before);
        if rows.is_empty() {
            return Ok(());
        }
        let output_bytes = rows.iter().try_fold(0_usize, |sum, row| {
            let charge = usize::try_from(crate::property_overlay::snapshot_charge(row))
                .map_err(|_| replay_resource_limit("property replay output row bytes"))?;
            sum.checked_add(charge.saturating_mul(3))
                .ok_or_else(|| replay_resource_limit("property replay output memory overflow"))
        })?;
        let physical_schema = replay_property_resource_schema(&self.logical_schema);
        let encoder_bytes = crate::permanent_parquet::replay_encoder_buffers(
            &physical_schema,
            output_bytes / 3,
            rows.len(),
        )?;
        let maximum_row_bytes = usize::try_from(
            rows.iter()
                .map(crate::property_overlay::snapshot_charge)
                .max()
                .unwrap_or(0),
        )
        .map_err(|_| replay_resource_limit("property replay row byte bound overflows"))?;
        let chunk_metadata = crate::permanent_parquet::replay_metadata_bytes(
            &physical_schema,
            1,
            rows.len(),
            maximum_row_bytes,
        )?;
        let base_chunk_metadata = crate::permanent_parquet::replay_metadata_bytes(
            &physical_schema,
            1,
            self.limits.max_batch_rows,
            0,
        )?;
        let extra_metadata_bytes = fragment
            .as_ref()
            .map_or(0, |writer| writer.extra_metadata_bytes)
            .checked_add(chunk_metadata.saturating_sub(base_chunk_metadata))
            .ok_or_else(|| replay_resource_limit("property replay metadata overflow"))?;
        if self
            .overlay_bytes
            .checked_add(self.retained_target_bytes)
            .and_then(|bytes| bytes.checked_add(encoder_bytes))
            .and_then(|bytes| bytes.checked_add(extra_metadata_bytes))
            .and_then(|bytes| bytes.checked_add(output_bytes))
            .and_then(|bytes| bytes.checked_add(self.writer_reservation_bytes))
            .is_none_or(|bytes| bytes > self.limits.max_replay_memory_bytes)
        {
            return Err(replay_resource_limit(
                "property replay Arrow and writer memory bound exceeded",
            ));
        }
        let output = self.fragment_writer(fragment)?;
        output.extra_metadata_bytes = extra_metadata_bytes;
        for row in rows {
            write_replay_property_snapshot(
                &mut output.writer,
                &output.logical_schema,
                &output.physical_schema,
                row,
            )?;
        }
        output.writer.flush().map_err(pq_err)?;
        Ok(())
    }

    fn property_rows(
        &self,
        names: &[&str],
        retained_writer_bytes: usize,
    ) -> Result<ReplayPropertyRows, GfError> {
        let targets = parse_replay_property_uuids(names)?;
        let (mut baseline, _) = crate::property_overlay::read_replay_property_targets(
            self.inventory,
            self.kind,
            self.route,
            &targets,
            self.limits
                .max_replay_memory_bytes
                .checked_sub(self.overlay_bytes)
                .and_then(|bytes| bytes.checked_sub(self.retained_target_bytes))
                .and_then(|bytes| bytes.checked_sub(retained_writer_bytes))
                .ok_or_else(|| {
                    replay_resource_limit("property replay decoder has no available budget")
                })?,
        )?;
        let mut before = BTreeMap::new();
        let baseline_bytes = baseline.values().fold(0_usize, |sum, row| {
            sum.saturating_add(
                usize::try_from(crate::property_overlay::snapshot_charge(row))
                    .unwrap_or(usize::MAX),
            )
        });
        let resident_before_output = self
            .overlay_bytes
            .checked_add(self.retained_target_bytes)
            .and_then(|bytes| bytes.checked_add(retained_writer_bytes))
            .and_then(|bytes| bytes.checked_add(baseline_bytes))
            .ok_or_else(|| replay_resource_limit("property replay decoded memory overflow"))?;
        if resident_before_output > self.limits.max_replay_memory_bytes {
            return Err(replay_resource_limit(
                "property replay decoded memory bound exceeded",
            ));
        }
        let mut rows = Vec::with_capacity(names.len());
        for entity_uuid in names {
            let uuid = uuid::Uuid::parse_str(entity_uuid)
                .map_err(pq_err)?
                .into_bytes();
            let prior = baseline.remove(&uuid);
            if replay_entity_deleted(self.overlay, self.edge, entity_uuid) {
                if let Some(prior) = prior {
                    before.insert(uuid, prior);
                }
                rows.push(crate::PropertySnapshotRow {
                    uuid,
                    tombstone: true,
                    values: BTreeMap::new(),
                });
                continue;
            }
            let existed = prior.is_some();
            let mut values: HashMap<String, IrLiteral> = prior
                .as_ref()
                .map(|row| row.values.clone().into_iter().collect())
                .unwrap_or_default();
            let prior_values = values.clone();
            apply_streamed_property_ops(entity_uuid, self.route, self.operations, &mut values);
            if (existed || !values.is_empty()) && values != prior_values {
                if let Some(prior) = prior {
                    before.insert(uuid, prior);
                }
                rows.push(crate::PropertySnapshotRow {
                    uuid,
                    tombstone: false,
                    values: values.into_iter().collect(),
                });
            }
        }
        let retained_baseline_bytes = baseline.values().try_fold(0_usize, |sum, row| {
            let charge = usize::try_from(crate::property_overlay::snapshot_charge(row))
                .map_err(|_| replay_resource_limit("property replay baseline row bytes"))?;
            sum.checked_add(charge)
                .ok_or_else(|| replay_resource_limit("property replay baseline memory overflow"))
        })?;
        let before_bytes = before.values().try_fold(0_usize, |sum, row| {
            let charge = usize::try_from(crate::property_overlay::snapshot_charge(row))
                .map_err(|_| replay_resource_limit("property replay prior row bytes"))?;
            sum.checked_add(charge)
                .ok_or_else(|| replay_resource_limit("property replay prior memory overflow"))
        })?;
        let after_bytes = rows.iter().try_fold(0_usize, |sum, row| {
            let charge = usize::try_from(crate::property_overlay::snapshot_charge(row))
                .map_err(|_| replay_resource_limit("property replay updated row bytes"))?;
            sum.checked_add(charge)
                .ok_or_else(|| replay_resource_limit("property replay updated memory overflow"))
        })?;
        if resident_before_output
            .checked_sub(baseline_bytes)
            .and_then(|bytes| bytes.checked_add(retained_baseline_bytes))
            .and_then(|bytes| bytes.checked_add(before_bytes))
            .and_then(|bytes| bytes.checked_add(after_bytes))
            .is_none_or(|bytes| bytes > self.limits.max_replay_memory_bytes)
        {
            return Err(replay_resource_limit(
                "property replay schema prepass memory bound exceeded",
            ));
        }
        Ok((before, rows))
    }
}

fn open_replay_property_fragment(
    target: &Path,
    kind: crate::PropertyRouteKind,
    edge: bool,
    route: &str,
    max_batch_rows: usize,
    logical_schema: &SchemaRef,
) -> Result<ReplayPropertyFragmentWriter, GfError> {
    use crate::property_overlay::{
        PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
        PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD,
        PropertyFragmentId,
    };
    let component = crate::route_component::component(route);
    let prior_generation =
        crate::property_overlay::enumerate_property_fragments(target, kind, &component)?
            .last()
            .map_or(0, |fragment| fragment.id.generation);
    let generation = prior_generation
        .checked_add(1)
        .ok_or_else(|| GfError::Storage("property fragment generation overflow".into()))?;
    let mut metadata = logical_schema.metadata().clone();
    metadata.insert(
        PROPERTY_OVERLAY_FORMAT_KEY.into(),
        PROPERTY_OVERLAY_FORMAT.into(),
    );
    metadata.insert(PROPERTY_ROUTE_KEY.into(), route.to_owned());
    metadata.insert(PROPERTY_KIND_KEY.into(), kind.metadata_value().into());
    metadata.insert(PROPERTY_GENERATION_KEY.into(), generation.to_string());
    metadata.insert(PROPERTY_ORDINAL_KEY.into(), "0".into());
    let mut fields = logical_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.insert(
        1,
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    );
    let physical_schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let subdir = if edge {
        "edge_properties"
    } else {
        "properties"
    };
    let path = target.join(subdir).join(component).join(
        PropertyFragmentId {
            generation,
            ordinal: 0,
        }
        .file_name(),
    );
    fs::create_dir_all(path.parent().expect("fragment has parent"))
        .map_err(|error| io_err(&error))?;
    let output = fs::File::create(&path).map_err(|error| io_err(&error))?;
    let properties = replay_writer_properties(max_batch_rows);
    let writer = parquet::arrow::ArrowWriter::try_new(
        output,
        Arc::clone(&physical_schema),
        Some(properties),
    )
    .map_err(pq_err)?;
    Ok(ReplayPropertyFragmentWriter {
        extra_metadata_bytes: 0,
        logical_schema: logical_schema.as_ref().clone(),
        physical_schema,
        writer,
    })
}

fn stream_replay_property_route_with_table(
    target: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    edge: bool,
    route: &str,
    route_table: &mut crate::route_component::RouteTable,
) -> Result<(), GfError> {
    route_table.check_insert(route, 64 * 1024 * 1024, 100_000)?;
    let (kind, operations) = replay_property_route_context(overlay, edge);
    let target_names = replay_property_target_names(overlay, operations, edge, route);
    let touched_rows = u64::try_from(target_names.len()).unwrap_or(u64::MAX);
    let replay_work_rows = touched_rows
        .checked_mul(2)
        .ok_or_else(|| replay_resource_limit("property replay work rows overflow"))?;
    if replay_work_rows > limits.max_work_rows
        || target_names.len() > limits.max_records_per_run
        || limits.max_batch_rows == 0
    {
        return Err(replay_resource_limit(
            "property replay touched UUID bound exceeded",
        ));
    }
    let target_bytes = target_names.iter().fold(0_usize, |sum, uuid| {
        sum.saturating_add(uuid.len()).saturating_add(16)
    });
    if target_bytes > limits.max_replay_memory_bytes {
        return Err(replay_resource_limit(
            "property replay target memory bound exceeded",
        ));
    }
    let overlay_bytes = overlay.estimated_memory();
    let retained_target_bytes = target_names
        .len()
        .checked_mul(size_of::<&str>())
        .and_then(|bytes| bytes.checked_add(target_bytes))
        .ok_or_else(|| replay_resource_limit("property replay target memory overflow"))?;
    if overlay_bytes
        .checked_add(retained_target_bytes)
        .is_none_or(|bytes| bytes > limits.max_replay_memory_bytes)
    {
        return Err(replay_resource_limit(
            "property replay overlay and target memory bound exceeded",
        ));
    }
    let mut fragment = None;
    let target_names = target_names.into_iter().collect::<Vec<_>>();
    let authority = inventory.route_schema(kind, route);
    let schema_base = authority
        .clone()
        .unwrap_or_else(|| replay_property_base_schema(kind));
    let logical_schema = replay_property_schema(
        kind,
        route,
        schema_base.as_ref(),
        operations
            .iter()
            .filter(|((_, operation_route, _), _)| operation_route == route),
    )?;
    let mut context = ReplayPropertyRouteContext {
        target,
        inventory,
        overlay,
        operations,
        limits,
        kind,
        edge,
        route,
        overlay_bytes,
        retained_target_bytes,
        writer_reservation_bytes: 0,
        logical_schema: Arc::new(logical_schema),
    };
    let inferred = Arc::clone(&context.logical_schema);
    let mut authority = authority;
    for names in target_names.chunks(limits.max_batch_rows) {
        let (before, after) = context.property_rows(names, 0)?;
        authority = Some(crate::property_overlay::update_live_route_schema(
            kind,
            route,
            authority.as_ref(),
            Arc::clone(&inferred),
            &before,
            &after,
        )?);
    }
    context.logical_schema =
        authority.ok_or_else(|| pq_err("property replay route has no touched schema authority"))?;
    context.writer_reservation_bytes = replay_writer_reservation(
        &replay_property_resource_schema(&context.logical_schema),
        target_names.len(),
        0,
        limits.max_batch_rows,
    )?;
    for names in target_names.chunks(limits.max_batch_rows) {
        context.process_chunk(names, &mut fragment)?;
    }
    if fragment.is_none() {
        return Ok(());
    }
    fragment
        .take()
        .expect("property replay writer exists")
        .writer
        .close()
        .map_err(pq_err)?;
    route_table.insert(route, 64 * 1024 * 1024, 100_000)?;
    Ok(())
}

pub(super) fn replace_private_replay_route_table(
    target: &Path,
    table: &crate::route_component::RouteTable,
    properties_changed: bool,
    node_properties_changed: bool,
) -> Result<(), GfError> {
    let mut batch = RewriteBatch::new();
    batch.stage_named_control_bytes(
        &target.join(crate::route_component::TABLE_FILE),
        &table.encode(64 * 1024 * 1024)?,
        "semantic-routes.json.",
    )?;
    // Replayed fragments advance their route generation. Publish the matching
    // property high-water mark and invalidate node-property search state, so
    // the next ordinary mutation cannot reuse an existing fragment generation.
    crate::durable_rewrite::commit(
        batch,
        target,
        false,
        node_properties_changed,
        properties_changed,
        None,
    )?;
    crate::capture_graph_files(target)?;
    Ok(())
}

#[cfg(test)]
fn stream_replay_property_route(
    target: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    limits: crate::graph_delta_journal::GraphDeltaJournalLimits,
    edge: bool,
    route: &str,
    _scratch: &Path,
) -> Result<(), GfError> {
    let mut table = crate::route_component::owned::admit_owned_workspace(target)?;
    stream_replay_property_route_with_table(
        target, inventory, overlay, limits, edge, route, &mut table,
    )?;
    replace_private_replay_route_table(target, &table, true, !edge)
}

type ReplayPropertyOperations =
    std::collections::BTreeMap<(String, String, String), Option<IrLiteral>>;

fn replay_property_route_context(
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    edge: bool,
) -> (crate::PropertyRouteKind, &ReplayPropertyOperations) {
    if edge {
        (crate::PropertyRouteKind::Edge, &overlay.edge_properties)
    } else {
        (crate::PropertyRouteKind::Node, &overlay.node_properties)
    }
}

fn replay_property_base_schema(kind: crate::PropertyRouteKind) -> SchemaRef {
    let join_field = match kind {
        crate::PropertyRouteKind::Node => NODE_PROPERTY_UUID_FIELD,
        crate::PropertyRouteKind::Edge => EDGE_PROPERTY_UUID_FIELD,
    };
    Arc::new(Schema::new(vec![uuid_field(join_field)]))
}

fn replay_entity_deleted(
    overlay: &crate::graph_delta_journal::ReplayOverlay,
    edge: bool,
    uuid: &str,
) -> bool {
    if edge {
        overlay.edges.get(uuid).is_some_and(Option::is_none)
    } else {
        overlay.nodes.get(uuid).is_some_and(Option::is_none)
    }
}

fn replay_property_target_names<'a>(
    overlay: &'a crate::graph_delta_journal::ReplayOverlay,
    operations: &'a ReplayPropertyOperations,
    edge: bool,
    route: &str,
) -> std::collections::BTreeSet<&'a str> {
    let mut target_names = operations
        .keys()
        .filter(|(_, operation_route, _)| operation_route == route)
        .map(|(uuid, _, _)| uuid.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let deleted = if edge {
        overlay
            .edges
            .iter()
            .filter(|(_, row)| row.is_none())
            .map(|(uuid, _)| uuid.as_str())
            .collect::<Vec<_>>()
    } else {
        overlay
            .nodes
            .iter()
            .filter(|(_, row)| row.is_none())
            .map(|(uuid, _)| uuid.as_str())
            .collect::<Vec<_>>()
    };
    target_names.extend(deleted);
    target_names
}

fn write_replay_property_snapshot(
    writer: &mut parquet::arrow::ArrowWriter<fs::File>,
    logical_schema: &Schema,
    physical_schema: &SchemaRef,
    row: crate::PropertySnapshotRow,
) -> Result<(), GfError> {
    let values = row.values.into_iter().collect();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(
        FixedSizeBinaryArray::try_from_iter([row.uuid.to_vec()].into_iter()).map_err(pq_err)?,
    )];
    columns.push(Arc::new(BooleanArray::from(vec![row.tombstone])));
    let property_row = PropRow {
        node_uuid: row.uuid,
        props: values,
    };
    for field in logical_schema.fields().iter().skip(1) {
        let column_type = col_type_from_field(field).ok_or_else(|| {
            pq_err(format!(
                "unsupported canonical property type for {}",
                field.name()
            ))
        })?;
        columns.push(build_property_array(
            field.name(),
            column_type,
            std::slice::from_ref(&property_row),
        ));
    }
    let batch = RecordBatch::try_new(Arc::clone(physical_schema), columns).map_err(pq_err)?;
    writer.write(&batch).map_err(pq_err)
}

fn apply_streamed_property_ops(
    entity_uuid: &str,
    stem: &str,
    operations: &std::collections::BTreeMap<(String, String, String), Option<IrLiteral>>,
    props: &mut HashMap<String, IrLiteral>,
) {
    for ((uuid, operation_stem, key), value) in operations {
        if uuid != entity_uuid || operation_stem != stem {
            continue;
        }
        match value {
            Some(value) => {
                props.insert(key.clone(), value.clone());
            }
            None => {
                props.remove(key);
            }
        }
    }
}

fn replay_property_schema<'a>(
    kind: crate::PropertyRouteKind,
    route: &str,
    base: &Schema,
    operations: impl Iterator<Item = (&'a (String, String, String), &'a Option<IrLiteral>)>,
) -> Result<Schema, GfError> {
    let mut additions = BTreeMap::<String, ColType>::new();
    for ((_, _, key), value) in operations {
        let Some(value) = value else { continue };
        reject_map_property_value(key, value)?;
        let Some(value_type) = ColType::of(value) else {
            continue;
        };
        additions
            .entry(key.clone())
            .and_modify(|prior| {
                if *prior != value_type && prior.is_scalar() && value_type.is_scalar() {
                    *prior = ColType::HetScalar;
                }
            })
            .or_insert(value_type);
    }
    let inferred = Schema::new_with_metadata(
        std::iter::once(base.field(0).clone())
            .chain(
                additions
                    .into_iter()
                    .map(|(name, column_type)| Field::new(name, column_type.data_type(), true)),
            )
            .collect::<Vec<_>>(),
        base.metadata().clone(),
    );
    crate::property_overlay::merge_property_route_schemas(kind, route, [base, &inferred])
        .map(|schema| schema.as_ref().clone())
}

#[cfg(test)]
mod tests;
