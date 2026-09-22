//! Retained-data migration planning and legacy projection.

use super::super::{
    BTreeMap, BTreeSet, CompiledComposition, Digest, GfError, HashMap, LegacySemanticProjection,
    MAX_SEMANTIC_BINDINGS, MigrationEngine, OntologyModuleId, Path, PathBuf, QualifiedSymbol,
    SemanticMigrationOperation, SemanticMigrationPlan, SemanticMigrationPropertySchema,
    SemanticRouteKind, SemanticStorageBinding, SemanticStorageBindings, Sha256, SymbolKind,
    TransformKind, admitted_semantic_parquet, binding, binding_key, checked_semantic_storage_id,
    corrupt, hex, id_namespace, legacy_ambiguous, lineage_key, projection_key, qualified,
};

impl SemanticStorageBindings {
    /// Derive a complete retained-data migration plan from authored module
    /// migrations in `next`. The pinned graph is inspected for every removal;
    /// unknown transforms and undeclared version changes fail closed.
    ///
    /// Repeating this method against the same exact parent produces the same
    /// `plan_digest`, allowing preview and publication to compare authority.
    pub fn plan_retained_data_migration(
        previous_composition: &CompiledComposition,
        next: &CompiledComposition,
        previous: &Self,
        pinned_graph_root: &Path,
    ) -> Result<SemanticMigrationPlan, GfError> {
        Self::plan_retained_data_migration_identity_equivalent(
            previous_composition,
            next,
            previous,
            pinned_graph_root,
            &[],
        )
    }

    /// Plan a composition replacement including independently verified,
    /// document-equivalent module identity upgrades without authored transforms.
    #[allow(clippy::too_many_lines)] // one canonical derivation keeps digest inputs co-located
    pub fn plan_retained_data_migration_identity_equivalent(
        previous_composition: &CompiledComposition,
        next: &CompiledComposition,
        previous: &Self,
        pinned_graph_root: &Path,
        identity_equivalent: &[(OntologyModuleId, OntologyModuleId)],
    ) -> Result<SemanticMigrationPlan, GfError> {
        previous.validate_against(previous_composition)?;
        let (source_inventory, _) = crate::capture_graph_files(pinned_graph_root)?;
        let source_inventory_sha256 =
            hex(Sha256::digest(crate::encode_inventory(&source_inventory)?).into());
        let retained_rows_scanned = scan_retained_migration_rows(pinned_graph_root)?;
        let authority = crate::graph_projection::TransformRoutes::from_inventory(
            pinned_graph_root,
            source_inventory.clone(),
        )?;
        let mut projected = Self::project(next, None)?;
        let mut target_property_schemas = next
            .modules
            .iter()
            .flat_map(|module| {
                module.doc.properties.iter().map(|property| {
                    let symbol = QualifiedSymbol {
                        module: module.id.clone(),
                        kind: SymbolKind::Property,
                        local_id: format!("{}:{}", property.owner, property.name),
                    };
                    SemanticMigrationPropertySchema {
                        symbol,
                        arrow_data_type: format!(
                            "{:?}",
                            crate::schemas::property_type_to_arrow(&property.value_type)
                        ),
                        nullable: property.nullable,
                    }
                })
            })
            .collect::<Vec<_>>();
        target_property_schemas.sort_by_key(|schema| schema.symbol.display());
        let mut matched_next = BTreeSet::new();
        let mut operations = Vec::new();
        let mut assigned = BTreeMap::<(u8, u32), usize>::new();

        for prior in &previous.bindings {
            let old_module = previous_composition
                .modules
                .iter()
                .find(|module| module.id == prior.symbol.module)
                .ok_or_else(|| corrupt("migration source binding module is absent"))?;
            let next_module = next
                .modules
                .iter()
                .find(|module| module.id.ontology_id == prior.symbol.module.ontology_id);
            let Some(next_module) = next_module else {
                if binding_has_retained_data_with_authority(prior, pinned_graph_root, &authority)? {
                    return Err(corrupt("module removal would orphan retained data"));
                }
                operations.push(SemanticMigrationOperation::RemoveEmpty {
                    symbol: prior.symbol.clone(),
                    storage_id: prior.storage_id,
                });
                continue;
            };
            let equivalent = identity_equivalent
                .iter()
                .any(|(old, new)| old == &old_module.id && new == &next_module.id);
            let steps = if equivalent {
                let mut old_document = old_module.doc.clone();
                old_document.version.clone_from(&next_module.doc.version);
                old_document
                    .migrations
                    .clone_from(&next_module.doc.migrations);
                if old_document != next_module.doc {
                    return Err(corrupt(
                        "claimed identity-equivalent module changes its schema",
                    ));
                }
                Vec::new()
            } else {
                MigrationEngine::plan(
                    &old_module.id.authored_version,
                    &next_module.id.authored_version,
                    &next_module.doc.migrations,
                )
                .map_err(|error| corrupt(&format!("authored migration path is invalid: {error}")))?
            };
            let mut local_id = prior.symbol.local_id.clone();
            let mut owner = prior.owner.as_ref().map(|value| value.local_id.clone());
            let mut renamed_entity = false;
            let mut renamed_property = false;
            for step in &steps {
                match &step.transform_kind {
                    TransformKind::RenameType { old_name, new_name } => {
                        if prior.symbol.kind == SymbolKind::Entity && local_id == *old_name {
                            local_id.clone_from(new_name);
                            renamed_entity = true;
                        }
                        if owner.as_deref() == Some(old_name) {
                            owner = Some(new_name.clone());
                            if prior.symbol.kind == SymbolKind::Property {
                                let property = local_id
                                    .split_once(':')
                                    .map(|(_, property)| property)
                                    .ok_or_else(|| corrupt("migration property is malformed"))?;
                                local_id = format!("{new_name}:{property}");
                                renamed_property = true;
                            }
                        }
                    }
                    TransformKind::RenameProperty {
                        owner: step_owner,
                        old_name,
                        new_name,
                    } => {
                        let current_owner = owner.as_deref();
                        if prior.symbol.kind == SymbolKind::Property
                            && current_owner == Some(step_owner)
                            && local_id
                                .split_once(':')
                                .is_some_and(|(_, name)| name == old_name)
                        {
                            local_id = format!("{step_owner}:{new_name}");
                            renamed_property = true;
                        }
                    }
                    TransformKind::RemoveProperty {
                        owner: step_owner,
                        name,
                    } if prior.symbol.kind == SymbolKind::Property
                        && owner.as_deref() == Some(step_owner)
                        && local_id
                            .split_once(':')
                            .is_some_and(|(_, value)| value == name) =>
                    {
                        local_id.clear();
                    }
                    TransformKind::RemoveType { name }
                        if (prior.symbol.kind == SymbolKind::Entity && local_id == *name)
                            || owner.as_deref() == Some(name) =>
                    {
                        local_id.clear();
                    }
                    TransformKind::AddProperty { .. }
                    | TransformKind::AddType { .. }
                    | TransformKind::RemoveProperty { .. }
                    | TransformKind::RemoveType { .. } => {}
                    TransformKind::Unknown { raw } => {
                        return Err(corrupt(&format!(
                            "unsupported authored migration transform `{raw}`"
                        )));
                    }
                }
            }
            let next_index = (!local_id.is_empty())
                .then(|| {
                    projected.bindings.iter().position(|candidate| {
                        candidate.route_kind == prior.route_kind
                            && candidate.symbol.module == next_module.id
                            && candidate.symbol.local_id == local_id
                            && candidate
                                .owner
                                .as_ref()
                                .map(|value| value.local_id.as_str())
                                == owner.as_deref()
                    })
                })
                .flatten();
            let Some(next_index) = next_index else {
                if binding_has_retained_data_with_authority(prior, pinned_graph_root, &authority)? {
                    return Err(corrupt("migration removal would orphan retained data"));
                }
                operations.push(SemanticMigrationOperation::RemoveEmpty {
                    symbol: prior.symbol.clone(),
                    storage_id: prior.storage_id,
                });
                continue;
            };
            if !matched_next.insert(next_index)
                || assigned
                    .insert(
                        (id_namespace(prior.route_kind), prior.storage_id),
                        next_index,
                    )
                    .is_some()
            {
                return Err(corrupt(
                    "authored migration maps multiple bindings ambiguously",
                ));
            }
            projected.bindings[next_index].storage_id = prior.storage_id;
            let target = &projected.bindings[next_index];
            let operation = if renamed_entity {
                SemanticMigrationOperation::RenameEntity {
                    from: prior.symbol.clone(),
                    to: target.symbol.clone(),
                    storage_id: prior.storage_id,
                }
            } else if renamed_property {
                SemanticMigrationOperation::RenameProperty {
                    from: prior.symbol.clone(),
                    to: target.symbol.clone(),
                    from_owner: prior
                        .owner
                        .clone()
                        .ok_or_else(|| corrupt("property owner absent"))?,
                    to_owner: target
                        .owner
                        .clone()
                        .ok_or_else(|| corrupt("property owner absent"))?,
                    storage_id: prior.storage_id,
                }
            } else {
                SemanticMigrationOperation::Carry {
                    from: prior.symbol.clone(),
                    to: target.symbol.clone(),
                    from_owner: prior.owner.clone(),
                    to_owner: target.owner.clone(),
                    storage_id: prior.storage_id,
                }
            };
            operations.push(operation);
        }

        let mut used = previous.bindings.iter().fold(
            BTreeMap::<u8, BTreeSet<u32>>::new(),
            |mut map, binding| {
                map.entry(id_namespace(binding.route_kind))
                    .or_default()
                    .insert(binding.storage_id);
                map
            },
        );
        for (index, binding) in projected.bindings.iter_mut().enumerate() {
            if matched_next.contains(&index) {
                continue;
            }
            if binding.symbol.kind == SymbolKind::Property {
                let target_module = next
                    .modules
                    .iter()
                    .find(|module| module.id == binding.symbol.module)
                    .ok_or_else(|| corrupt("added property module is absent"))?;
                let (owner_name, property_name) = binding
                    .symbol
                    .local_id
                    .split_once(':')
                    .ok_or_else(|| corrupt("added property identity is malformed"))?;
                let property = target_module
                    .doc
                    .properties
                    .iter()
                    .find(|property| property.owner == owner_name && property.name == property_name)
                    .ok_or_else(|| corrupt("added property definition is absent"))?;
                if !property.nullable {
                    let owner = binding
                        .owner
                        .as_ref()
                        .ok_or_else(|| corrupt("added property owner is absent"))?;
                    let owner_kind = if owner.kind == SymbolKind::Entity {
                        SemanticRouteKind::Entity
                    } else {
                        SemanticRouteKind::Relation
                    };
                    let target_owner_id = operations.iter().find_map(|operation| match operation {
                        SemanticMigrationOperation::Carry { to, storage_id, .. }
                        | SemanticMigrationOperation::RenameEntity { to, storage_id, .. }
                            if to == owner =>
                        {
                            Some(*storage_id)
                        }
                        _ => None,
                    });
                    let prior_owner = previous.bindings.iter().find(|candidate| {
                        candidate.route_kind == owner_kind
                            && Some(candidate.storage_id) == target_owner_id
                    });
                    if let Some(prior_owner) = prior_owner
                        && binding_has_retained_data_with_authority(
                            prior_owner,
                            pinned_graph_root,
                            &authority,
                        )?
                    {
                        return Err(corrupt(
                            "non-null property addition requires a deterministic typed retained-data backfill",
                        ));
                    }
                }
            }
            let ids = used.entry(id_namespace(binding.route_kind)).or_default();
            let mut id = ids.iter().next_back().copied().unwrap_or(0);
            loop {
                id = id
                    .checked_add(1)
                    .ok_or_else(|| corrupt("migration id space exhausted"))?;
                checked_semantic_storage_id(id)?;
                if ids.insert(id) {
                    break;
                }
            }
            binding.storage_id = id;
            operations.push(SemanticMigrationOperation::AddEmpty {
                symbol: binding.symbol.clone(),
                storage_id: id,
            });
        }
        projected.bindings.sort_by_key(binding_key);
        projected.validate_against(next)?;
        operations.sort_by_key(|operation| serde_json::to_vec(operation).unwrap_or_default());
        let canonical = serde_json::to_vec(&(
            &previous_composition.fingerprint,
            &next.fingerprint,
            &projected,
            &operations,
            retained_rows_scanned,
            &source_inventory_sha256,
            &target_property_schemas,
        ))
        .map_err(|_| corrupt("migration plan cannot be encoded"))?;
        let plan_digest = hex(Sha256::digest(canonical).into());
        Ok(SemanticMigrationPlan {
            from_composition_fingerprint: previous_composition.fingerprint.clone(),
            to_composition_fingerprint: next.fingerprint.clone(),
            bindings: projected,
            operations,
            retained_rows_scanned,
            source_inventory_sha256,
            target_property_schemas,
            plan_digest,
        })
    }

    /// Whether one exact generation binding has retained physical rows or
    /// non-null property values in the pinned materialized graph.
    ///
    /// # Errors
    /// Fails closed for malformed, oversized, or unreadable Parquet authority.
    pub fn binding_has_retained_data(
        binding: &SemanticStorageBinding,
        graph_root: &Path,
    ) -> Result<bool, GfError> {
        binding_has_retained_data(binding, graph_root)
    }

    /// Inspect a legacy single-module layout without mutation. Multi-module or
    /// unqualified layouts that cannot prove one owner fail closed.
    #[allow(clippy::too_many_lines)] // one bounded scan keeps projection evidence co-located
    pub fn project_legacy_unambiguous(
        composition: &CompiledComposition,
        graph_root: &Path,
    ) -> Result<LegacySemanticProjection, GfError> {
        let [module] = composition.modules.as_slice() else {
            return Err(legacy_ambiguous(
                "legacy semantic projection requires exactly one ontology module",
            ));
        };
        let mut bindings = Vec::new();
        for (id, entity) in module.doc.entity_types.iter().enumerate() {
            let symbol = qualified(&module.id, SymbolKind::Entity, &entity.name);
            bindings.push(binding(
                SemanticRouteKind::Entity,
                u32::try_from(id).map_err(|_| corrupt("legacy entity id exceeds u32"))?,
                symbol,
                None,
            ));
        }
        for (id, relation) in module.doc.relation_types.iter().enumerate() {
            let symbol = qualified(&module.id, SymbolKind::Relation, &relation.name);
            bindings.push(binding(
                SemanticRouteKind::Relation,
                u32::try_from(id).map_err(|_| corrupt("legacy relation id exceeds u32"))?,
                symbol,
                None,
            ));
        }
        for (id, property) in module.doc.properties.iter().enumerate() {
            let owner_kind = if module
                .doc
                .entity_types
                .iter()
                .any(|entity| entity.name == property.owner)
            {
                SymbolKind::Entity
            } else if module
                .doc
                .relation_types
                .iter()
                .any(|relation| relation.name == property.owner)
            {
                SymbolKind::Relation
            } else {
                return Err(legacy_ambiguous("legacy property owner is not declared"));
            };
            let owner = qualified(&module.id, owner_kind, &property.owner);
            let symbol = qualified(
                &module.id,
                SymbolKind::Property,
                &format!("{}:{}", property.owner, property.name),
            );
            bindings.push(binding(
                if owner_kind == SymbolKind::Entity {
                    SemanticRouteKind::NodeProperty
                } else {
                    SemanticRouteKind::EdgeProperty
                },
                u32::try_from(id).map_err(|_| corrupt("legacy property id exceeds u32"))?,
                symbol,
                Some(owner),
            ));
        }
        let bindings = Self::new(composition.fingerprint.clone(), bindings)?;
        bindings.validate_against(composition)?;

        let valid_entity_ids = bindings
            .bindings
            .iter()
            .filter(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .map(|binding| binding.storage_id)
            .collect::<BTreeSet<_>>();
        let mut topology_rows_scanned = 0_u64;
        let mut max_topology_batch_rows = 0_usize;
        for topology_path in crate::mutator::node_parquet_files(graph_root)? {
            use arrow::array::{Array, ListArray, UInt32Array};
            use arrow::datatypes::DataType;
            let reader = admitted_semantic_parquet(&topology_path)?
                .with_batch_size(8192)
                .build()
                .map_err(|_| legacy_ambiguous("legacy topology reader cannot be built"))?;
            for batch in reader {
                let batch =
                    batch.map_err(|_| legacy_ambiguous("legacy topology batch is invalid"))?;
                topology_rows_scanned = topology_rows_scanned
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| corrupt("legacy topology row count overflows"))?;
                max_topology_batch_rows = max_topology_batch_rows.max(batch.num_rows());
                if let Some(type_ids) = batch
                    .column_by_name("type_ids")
                    .and_then(|array| array.as_any().downcast_ref::<ListArray>())
                {
                    if type_ids.value_type() != DataType::UInt32 {
                        return Err(legacy_ambiguous("legacy topology type_ids has wrong type"));
                    }
                    for row in 0..type_ids.len() {
                        let values = type_ids.value(row);
                        let values = values.as_any().downcast_ref::<UInt32Array>().unwrap();
                        if values
                            .values()
                            .iter()
                            .any(|id| !valid_entity_ids.contains(id))
                        {
                            return Err(legacy_ambiguous(
                                "legacy topology contains an undeclared entity id",
                            ));
                        }
                    }
                }
            }
        }

        let (inventory, _) = crate::capture_graph_files(graph_root)?;
        let authority = crate::graph_projection::TransformRoutes::from_inventory(
            graph_root,
            inventory.clone(),
        )?;
        let logical_paths = inventory
            .files
            .iter()
            .map(|entry| {
                Ok((
                    authority.semantic_path(&entry.relative_path)?,
                    PathBuf::from(&entry.relative_path),
                ))
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        let mut route_moves = Vec::new();
        let mut destinations = crate::route_component::RouteTable::default();
        for binding in &bindings.bindings {
            let (domain, name) = match binding.route_kind {
                SemanticRouteKind::Entity => continue,
                SemanticRouteKind::Relation => ("topology/edges", binding.symbol.local_id.as_str()),
                SemanticRouteKind::NodeProperty => (
                    "properties",
                    binding.owner.as_ref().unwrap().local_id.as_str(),
                ),
                SemanticRouteKind::EdgeProperty => (
                    "edge_properties",
                    binding.owner.as_ref().unwrap().local_id.as_str(),
                ),
            };
            let flat = format!("{domain}/{name}.parquet");
            let prefix = format!("{domain}/{name}/");
            for (logical, physical) in &logical_paths {
                let destination = if logical == &flat {
                    Some(format!("{domain}/{}.parquet", binding.route))
                } else {
                    logical
                        .strip_prefix(&prefix)
                        .map(|fragment| format!("{domain}/{}/{fragment}", binding.route))
                };
                if let Some(destination) = destination
                    && !route_moves.iter().any(|(prior, _)| prior == physical)
                {
                    let encoded = crate::graph_projection::encode_transform_path(
                        &destination,
                        &mut destinations,
                    )?;
                    route_moves.push((physical.clone(), PathBuf::from(encoded)));
                }
            }
        }
        route_moves.sort();
        Ok(LegacySemanticProjection {
            bindings,
            route_moves,
            topology_rows_scanned,
            max_topology_batch_rows,
        })
    }

    /// Project a complete composition closure, carrying stable IDs from the
    /// prior generation and allocating new IDs monotonically without reuse.
    pub fn project(
        composition: &CompiledComposition,
        previous: Option<&Self>,
    ) -> Result<Self, GfError> {
        Self::project_with_removal_scan(composition, previous, None, &[])
    }

    /// Project while permitting removed bindings only when an exact pinned
    /// graph scan proves their prior physical identity has no retained data.
    pub fn project_with_graph_scan(
        composition: &CompiledComposition,
        previous: Option<&Self>,
        graph_root: &Path,
    ) -> Result<Self, GfError> {
        Self::project_with_removal_scan(composition, previous, Some(graph_root), &[])
    }

    /// Project with graph scanning and Rust-verified schema-identical module upgrades.
    pub fn project_with_graph_scan_identity_equivalent(
        composition: &CompiledComposition,
        previous: Option<&Self>,
        graph_root: &Path,
        identity_equivalent: &[(OntologyModuleId, OntologyModuleId)],
    ) -> Result<Self, GfError> {
        Self::project_with_removal_scan(
            composition,
            previous,
            Some(graph_root),
            identity_equivalent,
        )
    }

    #[allow(clippy::too_many_lines)] // projection is a single fail-closed authority calculation
    fn project_with_removal_scan(
        composition: &CompiledComposition,
        previous: Option<&Self>,
        graph_root: Option<&Path>,
        identity_equivalent: &[(OntologyModuleId, OntologyModuleId)],
    ) -> Result<Self, GfError> {
        if let Some(previous) = previous
            && previous.composition_fingerprint == composition.fingerprint
        {
            previous.validate_against(composition)?;
            return Ok(previous.clone());
        }
        if let Some(previous) = previous {
            for prior in &previous.bindings {
                let Some(next) = composition
                    .modules
                    .iter()
                    .find(|module| module.id.ontology_id == prior.symbol.module.ontology_id)
                else {
                    continue;
                };
                if next.id != prior.symbol.module {
                    let declared = next.doc.migrations.iter().any(|migration| {
                        migration.from_version == prior.symbol.module.authored_version
                            && migration.to_version == next.id.authored_version
                    });
                    let verified_equivalent = identity_equivalent
                        .iter()
                        .any(|(old, new)| old == &prior.symbol.module && new == &next.id);
                    if !declared && !verified_equivalent {
                        return Err(corrupt(
                            "module upgrade would carry stored IDs without an explicit authored migration lineage",
                        ));
                    }
                }
            }
        }
        let carried = previous
            .map(|value| {
                value
                    .bindings
                    .iter()
                    .map(|binding| {
                        (
                            lineage_key(
                                binding.route_kind,
                                &binding.symbol,
                                binding.owner.as_ref(),
                            ),
                            binding.storage_id,
                        )
                    })
                    .try_fold(HashMap::new(), |mut map, (key, id)| {
                        if map.insert(key, id).is_some() {
                            return Err(corrupt("semantic storage lineage is ambiguous"));
                        }
                        Ok(map)
                    })
            })
            .transpose()?
            .unwrap_or_default();
        let mut used = previous
            .map(|value| {
                value.bindings.iter().fold(
                    BTreeMap::<u8, BTreeSet<u32>>::new(),
                    |mut used, binding| {
                        used.entry(id_namespace(binding.route_kind))
                            .or_default()
                            .insert(binding.storage_id);
                        used
                    },
                )
            })
            .unwrap_or_default();
        let mut allocate = |key: &(SemanticRouteKind, QualifiedSymbol, Option<QualifiedSymbol>)| -> Result<u32, GfError> {
            let key = lineage_key(key.0, &key.1, key.2.as_ref());
            if let Some(id) = carried.get(&key) {
                return Ok(*id);
            }
            let namespace = id_namespace(key.0);
            let ids = used.entry(namespace).or_default();
            let mut next = ids.iter().next_back().copied().unwrap_or(0);
            loop {
                next = next
                    .checked_add(1)
                    .ok_or_else(|| corrupt("semantic storage id space is exhausted"))?;
                checked_semantic_storage_id(next)?;
                if ids.insert(next) {
                    return Ok(next);
                }
            }
        };
        let mut keys = Vec::new();
        for module in &composition.modules {
            for symbol in &module.symbols {
                match symbol.kind {
                    SymbolKind::Entity => {
                        keys.push((SemanticRouteKind::Entity, symbol.clone(), None));
                    }
                    SymbolKind::Relation => {
                        keys.push((SemanticRouteKind::Relation, symbol.clone(), None));
                    }
                    SymbolKind::Property => {
                        let owner_name = symbol
                            .local_id
                            .split_once(':')
                            .map(|(owner, _)| owner)
                            .ok_or_else(|| corrupt("property symbol has no qualified owner"))?;
                        let owner_kind = if module
                            .doc
                            .entity_types
                            .iter()
                            .any(|entity| entity.name == owner_name)
                        {
                            SymbolKind::Entity
                        } else if module
                            .doc
                            .relation_types
                            .iter()
                            .any(|relation| relation.name == owner_name)
                        {
                            SymbolKind::Relation
                        } else {
                            return Err(corrupt("property owner is absent from composition"));
                        };
                        let owner = QualifiedSymbol {
                            module: symbol.module.clone(),
                            kind: owner_kind,
                            local_id: owner_name.to_owned(),
                        };
                        keys.push((
                            if owner_kind == SymbolKind::Entity {
                                SemanticRouteKind::NodeProperty
                            } else {
                                SemanticRouteKind::EdgeProperty
                            },
                            symbol.clone(),
                            Some(owner),
                        ));
                    }
                    SymbolKind::Constraint | SymbolKind::Migration => {}
                }
            }
        }
        keys.sort_by_key(|(kind, symbol, owner)| projection_key(*kind, symbol, owner.as_ref()));
        let next_lineages = keys
            .iter()
            .map(|(kind, symbol, owner)| lineage_key(*kind, symbol, owner.as_ref()))
            .collect::<BTreeSet<_>>();
        let mut removal_authority = None;
        if let Some(previous) = previous {
            for removed in previous.bindings.iter().filter(|binding| {
                !next_lineages.contains(&lineage_key(
                    binding.route_kind,
                    &binding.symbol,
                    binding.owner.as_ref(),
                ))
            }) {
                let root = graph_root.ok_or_else(|| {
                    corrupt("semantic binding removal requires an exact pinned graph scan")
                })?;
                if removal_authority.is_none() {
                    removal_authority =
                        Some(crate::graph_projection::TransformRoutes::capture(root)?);
                }
                if binding_has_retained_data_with_authority(
                    removed,
                    root,
                    removal_authority.as_ref().expect("admitted above"),
                )? {
                    return Err(corrupt(
                        "semantic binding removal would orphan retained data",
                    ));
                }
            }
        }
        if keys.len() > MAX_SEMANTIC_BINDINGS {
            return Err(corrupt("semantic binding count exceeds limit"));
        }
        let mut bindings = Vec::with_capacity(keys.len());
        for (route_kind, symbol, owner) in keys {
            let storage_id = allocate(&(route_kind, symbol.clone(), owner.clone()))?;
            bindings.push(SemanticStorageBinding {
                route_kind,
                storage_id,
                route: Self::opaque_route(route_kind, &symbol, owner.as_ref()),
                symbol,
                owner,
            });
        }
        let value = Self::new(composition.fingerprint.clone(), bindings)?;
        value.validate_against(composition)?;
        Ok(value)
    }
}

fn scan_retained_migration_rows(graph_root: &Path) -> Result<u64, GfError> {
    let (inventory, _) = crate::capture_graph_files(graph_root)?;
    let mut rows = 0_u64;
    for entry in inventory.files {
        if Path::new(&entry.relative_path)
            .extension()
            .and_then(|value| value.to_str())
            != Some("parquet")
        {
            continue;
        }
        let path = graph_root.join(entry.relative_path);
        let reader = admitted_semantic_parquet(&path)?
            .with_batch_size(8_192)
            .build()
            .map_err(|_| corrupt("migration impact reader cannot be built"))?;
        for batch in reader {
            let batch = batch.map_err(|_| corrupt("migration impact batch is invalid"))?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| corrupt("migration impact row count overflows"))?;
        }
    }
    Ok(rows)
}

fn binding_has_retained_data(
    binding: &SemanticStorageBinding,
    graph_root: &Path,
) -> Result<bool, GfError> {
    let authority = crate::graph_projection::TransformRoutes::capture(graph_root)?;
    binding_has_retained_data_with_authority(binding, graph_root, &authority)
}

fn binding_has_retained_data_with_authority(
    binding: &SemanticStorageBinding,
    graph_root: &Path,
    authority: &crate::graph_projection::TransformRoutes,
) -> Result<bool, GfError> {
    if binding.route_kind == SemanticRouteKind::Entity {
        use arrow::array::{Array, ListArray, UInt32Array};
        for path in crate::catalog::topology_node_files(graph_root)? {
            let reader = admitted_semantic_parquet(&path)?
                .with_batch_size(8192)
                .build()
                .map_err(|_| corrupt("removal topology reader cannot be built"))?;
            for batch in reader {
                let batch = batch.map_err(|_| corrupt("removal topology cannot be read"))?;
                if let Some(values) = batch
                    .column_by_name("type_ids")
                    .and_then(|array| array.as_any().downcast_ref::<ListArray>())
                {
                    for row in 0..values.len() {
                        if values.is_null(row) {
                            return Err(corrupt("removal topology type_ids is null"));
                        }
                        let ids = values.value(row);
                        let ids = ids
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .ok_or_else(|| corrupt("removal topology type_ids has wrong type"))?;
                        if ids.values().contains(&binding.storage_id) {
                            return Ok(true);
                        }
                    }
                } else {
                    let primary = batch
                        .column_by_name("type_id")
                        .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
                        .ok_or_else(|| corrupt("removal topology labels are malformed"))?;
                    if primary.values().contains(&binding.storage_id) {
                        return Ok(true);
                    }
                }
            }
        }
        return Ok(false);
    }
    if matches!(
        binding.route_kind,
        SemanticRouteKind::NodeProperty | SemanticRouteKind::EdgeProperty
    ) {
        let column = binding
            .symbol
            .local_id
            .split_once(':')
            .map(|(_, property)| property)
            .ok_or_else(|| corrupt("removal property has no qualified column"))?;
        let batches = authority.property_batches(
            graph_root,
            &binding.route,
            binding.route_kind == SemanticRouteKind::EdgeProperty,
        )?;
        for batch in batches {
            let Some(values) = batch.column_by_name(column) else {
                continue;
            };
            if values.null_count() < values.len() {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    let paths = match binding.route_kind {
        SemanticRouteKind::Relation => authority
            .properties
            .edge_files(Some(&binding.route))
            .into_iter()
            .map(|(_, path)| path)
            .collect::<Vec<_>>(),
        SemanticRouteKind::Entity
        | SemanticRouteKind::NodeProperty
        | SemanticRouteKind::EdgeProperty => unreachable!("handled above"),
    };
    for path in paths {
        let builder = admitted_semantic_parquet(&path)?;
        if builder.metadata().file_metadata().num_rows() > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}
