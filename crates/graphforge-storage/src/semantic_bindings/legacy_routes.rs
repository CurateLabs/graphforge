//! Authenticated legacy route installation and rollback guards.

use super::{
    BTreeMap, Digest, File, GfError, MAX_SEMANTIC_PARQUET_COLUMNS, Path, PathBuf,
    SEMANTIC_COMPOSITION_METADATA_KEY, SEMANTIC_ROUTE_METADATA_KEY, SemanticRouteKind,
    SemanticStorageBinding, SemanticStorageBindings, Sha256, admitted_semantic_parquet, corrupt,
};

/// Rewrite a preflighted unambiguous legacy workspace to authenticated opaque
/// routes. The caller must hold graph publication authority and publish the
/// returned binding participant in the same generation. A local failure rolls
/// every completed rename back before returning.
pub struct LegacyRouteMigration {
    completed: Vec<(PathBuf, PathBuf, PathBuf)>,
    committed: bool,
    table: Option<LegacyTableRollback>,
    backup_root: PathBuf,
}

struct LegacyTableRollback {
    root: PathBuf,
    previous: Vec<u8>,
    installed: File,
    digest: [u8; 32],
}

impl LegacyTableRollback {
    fn still_owned(&self) -> bool {
        use std::io::Read;
        let Ok(root) = graphforge_filesystem::StableDirectory::open(&self.root) else {
            return false;
        };
        let Ok(mut current) =
            root.open_child_file(std::ffi::OsStr::new(crate::route_component::TABLE_FILE))
        else {
            return false;
        };
        let (Ok(actual), Ok(expected)) = (
            graphforge_filesystem::file_identity(&current),
            graphforge_filesystem::file_identity(&self.installed),
        ) else {
            return false;
        };
        if actual != expected || graphforge_filesystem::file_link_count(&current).ok() != Some(1) {
            return false;
        }
        let mut bytes = Vec::new();
        if current
            .by_ref()
            .take(64 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .is_err()
        {
            return false;
        }
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        digest == self.digest
    }
}

fn replace_legacy_table(root: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let mut batch = crate::RewriteBatch::new();
    batch.stage_named_control_bytes(
        &root.join(crate::route_component::TABLE_FILE),
        bytes,
        "semantic-routes.json.",
    )?;
    crate::durable_rewrite::commit(batch, root, false, false, false, None)?;
    Ok(())
}

impl LegacyRouteMigration {
    /// Keep the opaque routes after their binding participant is published.
    pub fn commit(&mut self) {
        for (_, _, backup) in &self.completed {
            let _ = std::fs::remove_file(backup);
        }
        let _ = std::fs::remove_dir(&self.backup_root);
        self.committed = true;
    }
}

impl Drop for LegacyRouteMigration {
    fn drop(&mut self) {
        if !self.committed {
            if self
                .table
                .as_ref()
                .is_some_and(|table| !table.still_owned())
            {
                return;
            }
            for (old, new, backup) in self.completed.iter().rev() {
                if backup.try_exists().unwrap_or(false) {
                    let _ = std::fs::remove_file(new);
                    let _ = std::fs::rename(backup, old);
                }
            }
            if let Some(table) = &self.table {
                let _ = replace_legacy_table(&table.root, &table.previous);
            }
            let _ = std::fs::remove_dir(&self.backup_root);
        }
    }
}

type PreparedLegacyRouteMoves<'a> = (
    Vec<(String, String, &'a SemanticStorageBinding)>,
    BTreeMap<String, String>,
);

fn prepare_legacy_route_moves<'a>(
    graph_root: &Path,
    route_moves: &[(PathBuf, PathBuf)],
    bindings: &'a SemanticStorageBindings,
) -> Result<PreparedLegacyRouteMoves<'a>, GfError> {
    let (before, _) = crate::capture_graph_files(graph_root)?;
    let authority =
        crate::graph_projection::TransformRoutes::from_inventory(graph_root, before.clone())?;
    let source_names = before
        .files
        .iter()
        .map(|entry| {
            Ok((
                PathBuf::from(&entry.relative_path),
                authority.semantic_path(&entry.relative_path)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, GfError>>()?;
    let mut prepared = Vec::new();
    let mut moves = BTreeMap::new();
    for (old, new) in route_moves {
        let logical_old = source_names
            .get(old)
            .ok_or_else(|| corrupt("legacy migration source is absent"))?;
        let new_wire = new
            .to_str()
            .ok_or_else(|| corrupt("legacy destination is not UTF-8"))?
            .replace('\\', "/");
        let mut matched = None;
        for binding in &bindings.bindings {
            let domain = match binding.route_kind {
                SemanticRouteKind::Entity => continue,
                SemanticRouteKind::Relation => "topology/edges",
                SemanticRouteKind::NodeProperty => "properties",
                SemanticRouteKind::EdgeProperty => "edge_properties",
            };
            let component = crate::route_component::component(&binding.route);
            let logical = if new_wire == format!("{domain}/{component}.parquet") {
                Some(format!("{domain}/{}.parquet", binding.route))
            } else {
                new_wire
                    .strip_prefix(&format!("{domain}/{component}/"))
                    .map(|fragment| format!("{domain}/{}/{fragment}", binding.route))
            };
            if let Some(logical) = logical {
                matched = Some((binding, logical));
                break;
            }
        }
        let (binding, logical_new) =
            matched.ok_or_else(|| corrupt("legacy migration destination has no binding"))?;
        if moves
            .insert(logical_old.clone(), logical_new.clone())
            .is_some()
        {
            return Err(corrupt("legacy migration repeats a source"));
        }
        prepared.push((logical_old.clone(), logical_new, binding));
    }
    Ok((prepared, moves))
}

fn rewrite_legacy_route(
    backup: &Path,
    new: &Path,
    binding: &SemanticStorageBinding,
    bindings: &SemanticStorageBindings,
) -> Result<(), GfError> {
    let builder = admitted_semantic_parquet(backup)?;
    if builder.schema().fields().len() > MAX_SEMANTIC_PARQUET_COLUMNS {
        return Err(corrupt("legacy migration column count exceeds limit"));
    }
    let mut metadata = builder.schema().metadata().clone();
    metadata.insert(SEMANTIC_ROUTE_METADATA_KEY.into(), binding.route.clone());
    if metadata.contains_key(crate::property_overlay::PROPERTY_OVERLAY_FORMAT_KEY) {
        metadata.insert(
            crate::property_overlay::PROPERTY_ROUTE_KEY.into(),
            binding.route.clone(),
        );
    }
    metadata.insert(
        SEMANTIC_COMPOSITION_METADATA_KEY.into(),
        bindings.composition_fingerprint.clone(),
    );
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
        builder.schema().fields().clone(),
        metadata,
    ));
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(new).map_err(|_| corrupt("legacy migration destination cannot open"))?,
        schema,
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .map_err(|_| corrupt("legacy migration writer cannot be built"))?;
    for batch in builder
        .with_batch_size(8192)
        .build()
        .map_err(|_| corrupt("legacy migration reader cannot be built"))?
    {
        writer
            .write(&batch.map_err(|_| corrupt("legacy migration batch is invalid"))?)
            .map_err(|_| corrupt("legacy migration batch cannot be written"))?;
    }
    writer
        .close()
        .map_err(|_| corrupt("legacy migration output cannot close"))?;
    Ok(())
}

/// Apply deterministic preflighted route moves and return a rollback guard.
/// The caller commits the guard only after publishing the matching bindings.
pub fn apply_legacy_route_moves(
    graph_root: &Path,
    route_moves: &[(PathBuf, PathBuf)],
    bindings: &SemanticStorageBindings,
) -> Result<LegacyRouteMigration, GfError> {
    let (prepared, moves) = prepare_legacy_route_moves(graph_root, route_moves, bindings)?;
    // Physical layout admission is its own committed, semantics-preserving unit.
    // Rollback below restores this admitted baseline until semantic publication succeeds.
    let prior_table = crate::route_component::owned::admit_owned_workspace(graph_root)?;
    let previous_table = prior_table.encode(64 * 1024 * 1024)?;
    let (inventory, _) = crate::capture_graph_files(graph_root)?;
    let mut logical_to_physical = BTreeMap::new();
    let mut output_table = crate::route_component::RouteTable::default();
    for entry in &inventory.files {
        if entry.relative_path == crate::route_component::TABLE_FILE {
            continue;
        }
        let logical = prior_table.semantic_relative_path(&entry.relative_path)?;
        crate::graph_projection::encode_transform_path(
            moves.get(&logical).unwrap_or(&logical),
            &mut output_table,
        )?;
        logical_to_physical.insert(
            logical,
            crate::graph_files::resolve_v1_inventory_entry(graph_root, entry)?,
        );
    }
    let backup_root = graph_root.join(format!(
        ".semantic-legacy-backups-{}",
        graphforge_core::uuid::new_v7()
    ));
    std::fs::create_dir(&backup_root)
        .map_err(|_| corrupt("legacy backup directory cannot be created"))?;
    let mut migration = LegacyRouteMigration {
        completed: Vec::new(),
        committed: false,
        table: None,
        backup_root,
    };
    for (old_relative, new_relative, binding) in prepared {
        let old = logical_to_physical
            .get(&old_relative)
            .cloned()
            .ok_or_else(|| corrupt("legacy migration source is absent"))?;
        let destination =
            crate::graph_projection::encode_transform_path(&new_relative, &mut output_table)?;
        let new = graph_root.join(destination);
        if new.exists() {
            return Err(corrupt("legacy migration destination already exists"));
        }
        let parent = new
            .parent()
            .ok_or_else(|| corrupt("legacy migration destination has no parent"))?;
        std::fs::create_dir_all(parent)
            .map_err(|_| corrupt("legacy migration destination cannot be created"))?;
        let backup = migration
            .backup_root
            .join(format!("{}.parquet", migration.completed.len()));
        if backup.exists() {
            return Err(corrupt("legacy migration backup already exists"));
        }
        if let Err(error) = std::fs::rename(&old, &backup) {
            for (prior_old, prior_new, prior_backup) in migration.completed.iter().rev() {
                let _ = std::fs::remove_file(prior_new);
                let _ = std::fs::rename(prior_backup, prior_old);
            }
            return Err(corrupt(&format!("legacy migration rename failed: {error}")));
        }
        let rewrite = rewrite_legacy_route(&backup, &new, binding, bindings);
        if let Err(error) = rewrite {
            let _ = std::fs::remove_file(&new);
            let _ = std::fs::rename(&backup, &old);
            for (prior_old, prior_new, prior_backup) in migration.completed.iter().rev() {
                let _ = std::fs::remove_file(prior_new);
                let _ = std::fs::rename(prior_backup, prior_old);
            }
            return Err(error);
        }
        migration.completed.push((old, new, backup));
    }
    let bytes = output_table.encode(64 * 1024 * 1024)?;
    replace_legacy_table(graph_root, &bytes)?;
    let directory = graphforge_filesystem::StableDirectory::open(graph_root)
        .map_err(|_| corrupt("legacy route table root cannot be retained"))?;
    let installed = directory
        .open_child_file(std::ffi::OsStr::new(crate::route_component::TABLE_FILE))
        .map_err(|_| corrupt("legacy route table cannot be retained"))?;
    migration.table = Some(LegacyTableRollback {
        root: graph_root.to_path_buf(),
        previous: previous_table,
        installed,
        digest: Sha256::digest(bytes).into(),
    });
    Ok(migration)
}

#[cfg(test)]
mod tests;
