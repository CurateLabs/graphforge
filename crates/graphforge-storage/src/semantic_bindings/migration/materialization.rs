//! Private semantic migration materialization and route rewriting.

use super::super::{
    BTreeMap, Digest, File, GfError, MAX_SEMANTIC_PARQUET_COLUMNS, Path,
    SEMANTIC_COMPOSITION_METADATA_KEY, SEMANTIC_ROUTE_METADATA_KEY, SemanticMigrationEvidence,
    SemanticMigrationLimits, SemanticMigrationOperation, SemanticMigrationPlan, SemanticRouteKind,
    SemanticStorageBindings, Sha256, SymbolKind, admitted_semantic_parquet, corrupt, hex,
    semantic_route_fragments, semantic_route_from_wire,
};

#[cfg(test)]
use super::super::PathBuf;

/// Materialize a complete private candidate graph tree without touching the
/// pinned source. Every Parquet file is rewritten to authenticate the target
/// composition; renamed owner routes and property columns are changed in the
/// same bounded pass. Any error or cancellation removes the candidate.
#[allow(clippy::too_many_lines)] // one bounded pass keeps candidate cleanup fail-closed
pub fn materialize_semantic_migration(
    plan: &SemanticMigrationPlan,
    source_graph_root: &Path,
    destination_graph_root: &Path,
    limits: SemanticMigrationLimits,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<SemanticMigrationEvidence, GfError> {
    if destination_graph_root.exists() {
        return Err(corrupt("semantic migration candidate already exists"));
    }
    if limits.batch_rows == 0 || limits.batch_rows > 1_000_000 {
        return Err(corrupt("semantic migration batch bound is invalid"));
    }
    let (inventory, _) = crate::capture_graph_files(source_graph_root)?;
    let source_routes = crate::graph_projection::TransformRoutes::from_inventory(
        source_graph_root,
        inventory.clone(),
    )?;
    let inventory_sha256 = hex(Sha256::digest(crate::encode_inventory(&inventory)?).into());
    if inventory_sha256 != plan.source_inventory_sha256 {
        return Err(corrupt(
            "semantic migration source inventory differs from the planned pinned graph",
        ));
    }
    if inventory.file_count > limits.max_files
        || inventory.total_byte_length > limits.max_input_bytes
    {
        return Err(corrupt("semantic migration source exceeds resource limits"));
    }
    checkpoint()?;

    let mut route_moves = BTreeMap::<String, String>::new();
    let mut property_renames = BTreeMap::<String, BTreeMap<String, String>>::new();
    let mut target_field_nullability = BTreeMap::<(String, String), bool>::new();
    for schema in &plan.target_property_schemas {
        let binding = plan
            .bindings
            .bindings
            .iter()
            .find(|binding| binding.symbol == schema.symbol)
            .ok_or_else(|| corrupt("migration target property schema has no binding"))?;
        let name = schema
            .symbol
            .local_id
            .split_once(':')
            .map(|(_, name)| name)
            .ok_or_else(|| corrupt("migration target property identity is malformed"))?;
        target_field_nullability.insert((binding.route.clone(), name.into()), schema.nullable);
    }
    for operation in &plan.operations {
        let (from, to, from_owner, to_owner) = match operation {
            SemanticMigrationOperation::Carry {
                from,
                to,
                from_owner,
                to_owner,
                ..
            } => (from, to, from_owner.as_ref(), to_owner.as_ref()),
            SemanticMigrationOperation::RenameProperty {
                from,
                to,
                from_owner,
                to_owner,
                ..
            } => (from, to, Some(from_owner), Some(to_owner)),
            SemanticMigrationOperation::RenameEntity { from, to, .. } => (from, to, None, None),
            SemanticMigrationOperation::AddEmpty { .. }
            | SemanticMigrationOperation::RemoveEmpty { .. } => continue,
        };
        let route_kind = match (from.kind, from_owner.map(|owner| owner.kind)) {
            (SymbolKind::Entity, _) => SemanticRouteKind::Entity,
            (SymbolKind::Relation, _) => SemanticRouteKind::Relation,
            (SymbolKind::Property, Some(SymbolKind::Entity)) => SemanticRouteKind::NodeProperty,
            (SymbolKind::Property, Some(SymbolKind::Relation)) => SemanticRouteKind::EdgeProperty,
            _ => return Err(corrupt("migration operation has invalid symbol ownership")),
        };
        let old_route = SemanticStorageBindings::opaque_route(route_kind, from, from_owner);
        let new_route = SemanticStorageBindings::opaque_route(route_kind, to, to_owner);
        if route_moves
            .insert(old_route.clone(), new_route.clone())
            .is_some_and(|prior| prior != new_route)
        {
            return Err(corrupt("migration route mapping is ambiguous"));
        }
        if from.kind == SymbolKind::Property {
            let old_name = from
                .local_id
                .split_once(':')
                .map(|(_, name)| name)
                .ok_or_else(|| corrupt("migration source property is malformed"))?;
            let new_name = to
                .local_id
                .split_once(':')
                .map(|(_, name)| name)
                .ok_or_else(|| corrupt("migration target property is malformed"))?;
            if old_name != new_name {
                property_renames
                    .entry(old_route)
                    .or_default()
                    .insert(old_name.to_owned(), new_name.to_owned());
            }
        }
    }

    let destination_parent = destination_graph_root
        .parent()
        .ok_or_else(|| corrupt("semantic migration destination has no parent"))?;
    let staging_graph_root = destination_parent.join(format!(
        ".semantic-migration-{}",
        graphforge_core::uuid::new_v7()
    ));
    std::fs::create_dir_all(&staging_graph_root)
        .map_err(|_| corrupt("semantic migration candidate cannot be created"))?;
    let result = (|| {
        let mut output_table = crate::route_component::RouteTable::default();
        let mut files_materialized = 0_u64;
        let mut rows_rewritten = 0_u64;
        let mut max_batch_rows = 0_usize;
        for entry in &inventory.files {
            checkpoint()?;
            if entry.relative_path == crate::route_component::TABLE_FILE {
                continue;
            }
            let source = crate::graph_files::resolve_v1_inventory_entry(source_graph_root, entry)?;
            let semantic = source_routes.semantic_path(&entry.relative_path)?;
            let old_route = semantic_route_from_wire(&semantic);
            let target_relative = migrated_semantic_wire(&semantic, &route_moves);
            let target_relative = crate::graph_projection::encode_transform_path(
                &target_relative,
                &mut output_table,
            )?;
            let target = staging_graph_root.join(target_relative);
            std::fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| corrupt("migration destination has no parent"))?,
            )
            .map_err(|_| corrupt("migration destination parent cannot be created"))?;
            if source.extension().and_then(|value| value.to_str()) != Some("parquet") {
                std::fs::copy(&source, &target)
                    .map_err(|_| corrupt("migration source file cannot be copied"))?;
                files_materialized += 1;
                continue;
            }
            let builder = admitted_semantic_parquet(&source)?;
            if builder.schema().fields().len() > MAX_SEMANTIC_PARQUET_COLUMNS {
                return Err(corrupt("migration Parquet column count exceeds limit"));
            }
            let schema_metadata = builder.schema().metadata().clone();
            let authenticated_old_route = schema_metadata
                .get(SEMANTIC_ROUTE_METADATA_KEY)
                .map(String::as_str)
                .or(old_route);
            if let (Some(path_route), Some(metadata_route)) = (old_route, authenticated_old_route)
                && path_route != metadata_route
            {
                return Err(corrupt(
                    "migration Parquet route metadata disagrees with path",
                ));
            }
            let new_route = authenticated_old_route
                .and_then(|route| route_moves.get(route))
                .cloned();
            let renames = authenticated_old_route.and_then(|route| property_renames.get(route));
            let target_route = new_route.as_deref().or(authenticated_old_route);
            let fields = builder
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let target_field = renames
                        .and_then(|values| values.get(field.name()))
                        .map_or_else(
                            || field.as_ref().clone(),
                            |name| field.as_ref().clone().with_name(name),
                        );
                    let nullable = target_route
                        .and_then(|route| {
                            target_field_nullability
                                .get(&(route.to_owned(), target_field.name().to_owned()))
                        })
                        .copied();
                    nullable.map_or(target_field.clone(), |value| {
                        target_field.with_nullable(value)
                    })
                })
                .collect::<Vec<_>>();
            let mut metadata = schema_metadata;
            if let Some(renames) = renames {
                crate::property_overlay::rename_live_schema_summary(&mut metadata, renames)?;
            }
            metadata.insert(
                SEMANTIC_COMPOSITION_METADATA_KEY.into(),
                plan.to_composition_fingerprint.clone(),
            );
            if let Some(route) = new_route {
                metadata.insert(SEMANTIC_ROUTE_METADATA_KEY.into(), route.clone());
                if metadata.contains_key(crate::property_overlay::PROPERTY_OVERLAY_FORMAT_KEY) {
                    metadata.insert(crate::property_overlay::PROPERTY_ROUTE_KEY.into(), route);
                }
            }
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
                fields, metadata,
            ));
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&target).map_err(|_| corrupt("migration output cannot be opened"))?,
                schema.clone(),
                Some(crate::permanent_parquet::writer_properties().build()),
            )
            .map_err(|_| corrupt("migration writer cannot be built"))?;
            for batch in builder
                .with_batch_size(limits.batch_rows)
                .build()
                .map_err(|_| corrupt("migration reader cannot be built"))?
            {
                checkpoint()?;
                let batch = batch.map_err(|_| corrupt("migration Parquet batch is invalid"))?;
                rows_rewritten = rows_rewritten
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| corrupt("migration row count overflows"))?;
                if rows_rewritten > limits.max_rows {
                    return Err(corrupt("semantic migration row limit exceeded"));
                }
                max_batch_rows = max_batch_rows.max(batch.num_rows());
                let target_batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    batch.columns().to_vec(),
                )
                .map_err(|_| corrupt("migration renamed batch schema is invalid"))?;
                writer
                    .write(&target_batch)
                    .map_err(|_| corrupt("migration batch cannot be written"))?;
            }
            writer
                .close()
                .map_err(|_| corrupt("migration output cannot close"))?;
            files_materialized += 1;
        }
        crate::graph_projection::install_transform_table(&staging_graph_root, &output_table)?;
        files_materialized += 1;
        let (verified_source, _) = crate::capture_graph_files(source_graph_root)?;
        if hex(Sha256::digest(crate::encode_inventory(&verified_source)?).into())
            != plan.source_inventory_sha256
        {
            return Err(corrupt(
                "semantic migration source changed while materializing the private candidate",
            ));
        }
        plan.bindings
            .validate_physical_routes(&staging_graph_root)?;
        for property in &plan.target_property_schemas {
            let binding = plan
                .bindings
                .bindings
                .iter()
                .find(|binding| binding.symbol == property.symbol)
                .ok_or_else(|| corrupt("migration target property binding is absent"))?;
            let property_name = property
                .symbol
                .local_id
                .split_once(':')
                .map(|(_, name)| name)
                .ok_or_else(|| corrupt("migration target property identity is malformed"))?;
            for path in semantic_route_fragments(binding, &staging_graph_root)? {
                let builder = admitted_semantic_parquet(&path)?;
                match builder.schema().field_with_name(property_name) {
                    Ok(field)
                        if format!("{:?}", field.data_type()) != property.arrow_data_type
                            || field.is_nullable() != property.nullable =>
                    {
                        return Err(corrupt(
                            "migration target property column type or nullability disagrees",
                        ));
                    }
                    Err(_) if !property.nullable => {
                        return Err(corrupt(
                            "migration target required property column is missing",
                        ));
                    }
                    Ok(_) | Err(_) => {}
                }
            }
        }
        let (candidate, _) = crate::capture_graph_files(&staging_graph_root)?;
        let candidate_bytes = crate::encode_inventory(&candidate)?;
        Ok(SemanticMigrationEvidence {
            plan_digest: plan.plan_digest.clone(),
            files_materialized,
            rows_rewritten,
            max_batch_rows,
            candidate_inventory_sha256: hex(Sha256::digest(candidate_bytes).into()),
        })
    })();
    let evidence = match result {
        Ok(evidence) => evidence,
        Err(error) => {
            return match std::fs::remove_dir_all(&staging_graph_root) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(corrupt(&format!(
                    "semantic migration failed and private staging cleanup failed: {error}; {cleanup}"
                ))),
            };
        }
    };
    std::fs::rename(&staging_graph_root, destination_graph_root).map_err(|publish| {
        match std::fs::remove_dir_all(&staging_graph_root) {
            Ok(()) => corrupt(&format!(
                "semantic migration candidate publication failed: {publish}"
            )),
            Err(cleanup) => corrupt(&format!(
                "semantic migration candidate publication and cleanup failed: {publish}; {cleanup}"
            )),
        }
    })?;
    Ok(evidence)
}

fn migrated_semantic_wire(relative: &str, route_moves: &BTreeMap<String, String>) -> String {
    let Some(route) = semantic_route_from_wire(relative) else {
        return relative.to_owned();
    };
    let Some(new) = route_moves.get(route) else {
        return relative.to_owned();
    };
    let mut parts = relative.split('/').map(str::to_owned).collect::<Vec<_>>();
    let index = if parts[0] == "topology" { 2 } else { 1 };
    parts[index] = if parts.len() == index + 1 {
        format!("{new}.parquet")
    } else {
        new.clone()
    };
    parts.join("/")
}

#[cfg(test)]
fn semantic_route_from_relative(relative: &Path) -> Option<&str> {
    let parts = relative
        .components()
        .map(std::path::Component::as_os_str)
        .collect::<Vec<_>>();
    let route_index = match parts.as_slice() {
        [topology, edges, ..]
            if *topology == std::ffi::OsStr::new("topology")
                && *edges == std::ffi::OsStr::new("edges") =>
        {
            2
        }
        [properties, ..] if *properties == std::ffi::OsStr::new("properties") => 1,
        [properties, ..] if *properties == std::ffi::OsStr::new("edge_properties") => 1,
        _ => return None,
    };
    let route = parts.get(route_index)?.to_str()?;
    route
        .strip_suffix(".parquet")
        .or(Some(route))
        .filter(|route| route.starts_with("s-"))
}

#[cfg(test)]
pub(in crate::semantic_bindings) fn migrated_semantic_relative(
    relative: &Path,
    route_moves: &BTreeMap<String, String>,
) -> PathBuf {
    let Some(old_route) = semantic_route_from_relative(relative) else {
        return relative.to_path_buf();
    };
    let Some(new_route) = route_moves.get(old_route) else {
        return relative.to_path_buf();
    };
    let mut parts = relative
        .components()
        .map(|part| part.as_os_str().to_owned())
        .collect::<Vec<_>>();
    let route_index = if parts.first().is_some_and(|part| part == "topology") {
        2
    } else {
        1
    };
    let flat = parts[route_index]
        .to_str()
        .is_some_and(|part| part.ends_with(".parquet"));
    parts[route_index] = if flat {
        format!("{new_route}.parquet").into()
    } else {
        new_route.into()
    };
    parts.into_iter().collect()
}
