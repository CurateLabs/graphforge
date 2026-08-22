//! Generation-bound bindings between physical graph storage and qualified ontology authority.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_ontology::{
    CompiledComposition, MigrationEngine, OntologyModuleId, QualifiedSymbol, SymbolKind,
    TransformKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{GRAPH_CAPABILITY_ID, ProjectParticipant, ProjectParticipantEncoding};

/// Registered graph participant family.
pub const GRAPH_SEMANTIC_BINDINGS_FAMILY: &str = "semantic_bindings";
/// Frozen participant contract version.
pub const GRAPH_SEMANTIC_BINDINGS_VERSION: u32 = 1;
/// Maximum number of bindings accepted before allocation or serialization.
pub const MAX_SEMANTIC_BINDINGS: usize = 1_000_000;
/// Maximum canonical participant bytes.
pub const MAX_SEMANTIC_BINDING_BYTES: usize = 64 * 1024 * 1024;
const MAX_SEMANTIC_PARQUET_COLUMNS: usize = 4_096;
const MAX_SEMANTIC_PARQUET_METADATA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SEMANTIC_STRING_BYTES: usize = 4_096;
/// Parquet schema metadata key authenticating the opaque route.
pub const SEMANTIC_ROUTE_METADATA_KEY: &str = "graphforge.semantic_route";
/// Parquet schema metadata key authenticating the owning composition.
pub const SEMANTIC_COMPOSITION_METADATA_KEY: &str = "graphforge.composition_fingerprint";

/// Physical storage route class.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticRouteKind {
    /// Node topology type binding.
    Entity,
    /// Typed edge physical route.
    Relation,
    /// Entity-owned property route.
    NodeProperty,
    /// Relation-owned property route.
    EdgeProperty,
}

/// One exact physical binding. Runtime-tagged IDs are deliberately excluded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SemanticStorageBinding {
    /// Route class.
    pub route_kind: SemanticRouteKind,
    /// Untagged stable storage ID. IDs are never derived from runtime catalogs.
    pub storage_id: u32,
    /// Collision-free opaque route used in physical paths and Parquet metadata.
    pub route: String,
    /// Exact semantic symbol.
    pub symbol: QualifiedSymbol,
    /// Exact qualified owner for property routes.
    pub owner: Option<QualifiedSymbol>,
}

/// Canonical complete mapping for one composition-bound graph generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SemanticStorageBindings {
    /// Contract version.
    pub contract_version: u32,
    /// Exact composition fingerprint owning these bindings.
    pub composition_fingerprint: String,
    /// Canonically ordered unique bindings.
    pub bindings: Vec<SemanticStorageBinding>,
}

/// Deterministic, mutation-free plan for converting one unambiguous legacy
/// single-module graph into opaque semantic routes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LegacySemanticProjection {
    /// Exact projected authority using the IDs already persisted in topology.
    pub bindings: SemanticStorageBindings,
    /// Existing relative path to authenticated opaque relative path.
    pub route_moves: Vec<(PathBuf, PathBuf)>,
    /// Rows inspected with a fixed-size Parquet batch reader.
    pub topology_rows_scanned: u64,
    /// Largest resident topology batch used by the scanner.
    pub max_topology_batch_rows: usize,
}

/// One deterministic physical consequence of an authored ontology migration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SemanticMigrationOperation {
    /// Preserve a physical identity while its qualified authority is unchanged.
    Carry {
        /// Prior qualified symbol.
        from: QualifiedSymbol,
        /// New qualified symbol (normally only the module version changes).
        to: QualifiedSymbol,
        /// Prior qualified owner for property routes.
        from_owner: Option<QualifiedSymbol>,
        /// New qualified owner for property routes.
        to_owner: Option<QualifiedSymbol>,
        /// Stable physical identity.
        storage_id: u32,
    },
    /// Rename an entity while preserving every retained topology ID.
    RenameEntity {
        /// Prior qualified entity.
        from: QualifiedSymbol,
        /// New qualified entity.
        to: QualifiedSymbol,
        /// Stable physical identity.
        storage_id: u32,
    },
    /// Rename a property column and, when its owner changes, its opaque table route.
    RenameProperty {
        /// Prior qualified property.
        from: QualifiedSymbol,
        /// New qualified property.
        to: QualifiedSymbol,
        /// Prior qualified owner.
        from_owner: QualifiedSymbol,
        /// New qualified owner.
        to_owner: QualifiedSymbol,
        /// Stable physical identity.
        storage_id: u32,
    },
    /// Add a binding that has no retained values in the pinned parent.
    AddEmpty {
        /// New qualified symbol.
        symbol: QualifiedSymbol,
        /// Newly allocated physical identity.
        storage_id: u32,
    },
    /// Remove a binding after a pinned scan proved it has no retained values.
    RemoveEmpty {
        /// Removed qualified symbol.
        symbol: QualifiedSymbol,
        /// Retired physical identity (never reassigned by this plan).
        storage_id: u32,
    },
}

/// Target property schema authenticated by a migration plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SemanticMigrationPropertySchema {
    /// Qualified target property.
    pub symbol: QualifiedSymbol,
    /// Arrow data type debug identity produced by the Rust schema authority.
    pub arrow_data_type: String,
    /// Required Arrow field nullability.
    pub nullable: bool,
}

/// Canonical Rust-derived plan for an atomic retained-data ontology migration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SemanticMigrationPlan {
    /// Exact pinned parent composition.
    pub from_composition_fingerprint: String,
    /// Exact requested composition.
    pub to_composition_fingerprint: String,
    /// Complete post-migration bindings.
    pub bindings: SemanticStorageBindings,
    /// Canonically ordered physical consequences.
    pub operations: Vec<SemanticMigrationOperation>,
    /// Rows inspected from the exact pinned parent while deriving data impact.
    pub retained_rows_scanned: u64,
    /// SHA-256 of the exact canonical pinned graph inventory.
    pub source_inventory_sha256: String,
    /// Canonical target property field contracts.
    pub target_property_schemas: Vec<SemanticMigrationPropertySchema>,
    /// SHA-256 of the canonical plan fields above.
    pub plan_digest: String,
}

/// Finite resource bounds for private retained-data migration materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticMigrationLimits {
    /// Maximum source files admitted.
    pub max_files: u64,
    /// Maximum aggregate source bytes admitted.
    pub max_input_bytes: u64,
    /// Maximum rows rewritten across Parquet files.
    pub max_rows: u64,
    /// Fixed maximum Arrow record batch size.
    pub batch_rows: usize,
}

impl Default for SemanticMigrationLimits {
    fn default() -> Self {
        Self {
            max_files: 100_000,
            max_input_bytes: 64 * 1024 * 1024 * 1024,
            max_rows: 1_000_000_000,
            batch_rows: 8_192,
        }
    }
}

/// Deterministic evidence from a fully materialized private candidate tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticMigrationEvidence {
    /// Exact plan applied.
    pub plan_digest: String,
    /// Files copied or rewritten.
    pub files_materialized: u64,
    /// Parquet rows rewritten in bounded batches.
    pub rows_rewritten: u64,
    /// Largest resident record batch.
    pub max_batch_rows: usize,
    /// Authenticated inventory digest of the complete candidate tree.
    pub candidate_inventory_sha256: String,
}

impl SemanticStorageBindings {
    /// Derive a complete retained-data migration plan from authored module
    /// migrations in `next`. The pinned graph is inspected for every removal;
    /// unknown transforms and undeclared version changes fail closed.
    ///
    /// Repeating this method against the same exact parent produces the same
    /// `plan_digest`, allowing preview and publication to compare authority.
    #[allow(clippy::too_many_lines)] // one canonical derivation keeps digest inputs co-located
    pub fn plan_retained_data_migration(
        previous_composition: &CompiledComposition,
        next: &CompiledComposition,
        previous: &Self,
        pinned_graph_root: &Path,
    ) -> Result<SemanticMigrationPlan, GfError> {
        previous.validate_against(previous_composition)?;
        let (source_inventory, _) = crate::capture_graph_files(pinned_graph_root)?;
        let source_inventory_sha256 =
            hex(Sha256::digest(crate::encode_inventory(&source_inventory)?).into());
        let retained_rows_scanned = scan_retained_migration_rows(pinned_graph_root)?;
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
                if binding_has_retained_data(prior, pinned_graph_root)? {
                    return Err(corrupt("module removal would orphan retained data"));
                }
                operations.push(SemanticMigrationOperation::RemoveEmpty {
                    symbol: prior.symbol.clone(),
                    storage_id: prior.storage_id,
                });
                continue;
            };
            let steps = MigrationEngine::plan(
                &old_module.id.authored_version,
                &next_module.id.authored_version,
                &next_module.doc.migrations,
            )
            .map_err(|error| corrupt(&format!("authored migration path is invalid: {error}")))?;
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
                if binding_has_retained_data(prior, pinned_graph_root)? {
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
                        && binding_has_retained_data(prior_owner, pinned_graph_root)?
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
                if id & ((1 << 30) | (1 << 31)) != 0 {
                    return Err(corrupt("migration id space reached reserved runtime tags"));
                }
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
        let topology_path = graph_root.join("topology/nodes.parquet");
        let mut topology_rows_scanned = 0_u64;
        let mut max_topology_batch_rows = 0_usize;
        if topology_path.exists() {
            use arrow::array::{Array, ListArray, UInt32Array};
            use arrow::datatypes::DataType;
            preflight_parquet_footer(&topology_path)?;
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(&topology_path)
                    .map_err(|_| legacy_ambiguous("legacy topology cannot be opened"))?,
            )
            .map_err(|_| legacy_ambiguous("legacy topology metadata is invalid"))?
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

        let mut route_moves = Vec::new();
        for binding in &bindings.bindings {
            let old = match binding.route_kind {
                SemanticRouteKind::Entity => continue,
                SemanticRouteKind::Relation => PathBuf::from("topology/edges")
                    .join(format!("{}.parquet", binding.symbol.local_id)),
                SemanticRouteKind::NodeProperty => PathBuf::from("properties").join(format!(
                    "{}.parquet",
                    binding.owner.as_ref().unwrap().local_id
                )),
                SemanticRouteKind::EdgeProperty => PathBuf::from("edge_properties").join(format!(
                    "{}.parquet",
                    binding.owner.as_ref().unwrap().local_id
                )),
            };
            let new = binding
                .physical_path(Path::new(""))
                .expect("routed binding");
            if graph_root.join(&old).exists() && !route_moves.iter().any(|(prior, _)| prior == &old)
            {
                route_moves.push((old, new));
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
        let mut allocate = |key: &(SemanticRouteKind, QualifiedSymbol, Option<QualifiedSymbol>)| {
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
                if next & ((1 << 30) | (1 << 31)) != 0 {
                    return Err(corrupt(
                        "semantic storage id space reached reserved runtime tags",
                    ));
                }
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
                if binding_has_retained_data(removed, root)? {
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

    /// Construct and validate a canonical mapping.
    pub fn new(
        composition_fingerprint: String,
        mut bindings: Vec<SemanticStorageBinding>,
    ) -> Result<Self, GfError> {
        bindings.sort_by_key(binding_key);
        let value = Self {
            contract_version: GRAPH_SEMANTIC_BINDINGS_VERSION,
            composition_fingerprint,
            bindings,
        };
        value.validate()?;
        Ok(value)
    }

    /// Stable opaque physical route for a qualified symbol.
    #[must_use]
    pub fn opaque_route(
        kind: SemanticRouteKind,
        symbol: &QualifiedSymbol,
        owner: Option<&QualifiedSymbol>,
    ) -> String {
        let mut digest = Sha256::new();
        digest.update(b"graphforge-semantic-route/1\0");
        let route_symbol = owner.unwrap_or(symbol);
        // Properties are columns in their owner's physical table.  The owner
        // binding and every property owned by it therefore authenticate one
        // exact route; property kind must never create a second table route.
        let table_kind = match kind {
            SemanticRouteKind::NodeProperty => SemanticRouteKind::Entity,
            SemanticRouteKind::EdgeProperty => SemanticRouteKind::Relation,
            other => other,
        };
        digest.update(route_kind_token(table_kind).as_bytes());
        digest.update(route_symbol.module.display_ref().as_bytes());
        digest.update([0]);
        digest.update(route_symbol.kind.as_str().as_bytes());
        digest.update([0]);
        digest.update(route_symbol.local_id.as_bytes());
        let encoded = hex(digest.finalize().into());
        format!("s-{encoded}")
    }

    /// Authenticate every binding against one exact compiled composition.
    pub fn validate_against(&self, composition: &CompiledComposition) -> Result<(), GfError> {
        if self.composition_fingerprint != composition.fingerprint {
            return Err(corrupt("semantic bindings target a different composition"));
        }
        let mut semantic = BTreeSet::new();
        for binding in &self.bindings {
            let module = composition
                .modules
                .iter()
                .find(|module| module.id == binding.symbol.module)
                .ok_or_else(|| corrupt("semantic binding module is absent"))?;
            if !module.symbols.contains(&binding.symbol) {
                return Err(corrupt("semantic binding symbol is absent"));
            }
            if !semantic.insert((binding.route_kind, binding.symbol.display())) {
                return Err(corrupt("semantic symbol has multiple physical bindings"));
            }
            if let Some(owner) = &binding.owner {
                if owner.module != binding.symbol.module {
                    return Err(corrupt("property binding crosses module ownership"));
                }
                let expected_kind = match binding.route_kind {
                    SemanticRouteKind::NodeProperty => SymbolKind::Entity,
                    SemanticRouteKind::EdgeProperty => SymbolKind::Relation,
                    _ => return Err(corrupt("non-property binding has an owner")),
                };
                if owner.kind != expected_kind || !module.symbols.contains(owner) {
                    return Err(corrupt(
                        "property binding owner is absent or has wrong kind",
                    ));
                }
                let declared = module.doc.properties.iter().any(|property| {
                    format!("{}:{}", property.owner, property.name) == binding.symbol.local_id
                        && property.owner == owner.local_id
                });
                if !declared {
                    return Err(corrupt("property binding is not declared by its owner"));
                }
            }
            if binding.storage_id & ((1 << 30) | (1 << 31)) != 0 {
                return Err(corrupt(
                    "runtime-tagged or reserved storage id is forbidden",
                ));
            }
        }
        let expected_count = composition
            .modules
            .iter()
            .try_fold(0usize, |count, module| {
                count
                    .checked_add(module.doc.entity_types.len())
                    .and_then(|count| count.checked_add(module.doc.relation_types.len()))
                    .and_then(|count| count.checked_add(module.doc.properties.len()))
                    .ok_or_else(|| corrupt("composition semantic closure count overflows"))
            })?;
        if self.bindings.len() != expected_count {
            return Err(corrupt(
                "semantic bindings do not cover the complete composition closure",
            ));
        }
        Ok(())
    }

    fn validate_topology_ids(&self, graph_root: &Path) -> Result<(), GfError> {
        use arrow::array::{Array, ListArray, UInt32Array};

        let path = graph_root.join("topology/nodes.parquet");
        if !path.exists() {
            return Ok(());
        }
        let entity_ids = self
            .bindings
            .iter()
            .filter(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .map(|binding| binding.storage_id)
            .collect::<BTreeSet<_>>();
        preflight_parquet_footer(&path)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).map_err(|_| corrupt("semantic topology cannot be opened"))?,
        )
        .map_err(|_| corrupt("semantic topology metadata is invalid"))?
        .with_batch_size(8192)
        .build()
        .map_err(|_| corrupt("semantic topology reader cannot be built"))?;
        for batch in reader {
            let batch = batch.map_err(|_| corrupt("semantic topology batch is invalid"))?;
            let primary = batch
                .column_by_name("type_id")
                .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
                .ok_or_else(|| corrupt("semantic topology type_id is missing or malformed"))?;
            let type_ids = batch
                .column_by_name("type_ids")
                .and_then(|array| array.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| corrupt("semantic topology type_ids is missing or malformed"))?;
            for row in 0..type_ids.len() {
                if type_ids.is_null(row) {
                    return Err(corrupt("semantic topology type_ids is null"));
                }
                let values = type_ids.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| corrupt("semantic topology type_ids has wrong element type"))?;
                let primary_id = primary.value(row);
                if !values.values().contains(&primary_id) {
                    return Err(corrupt(
                        "semantic topology scalar type_id is absent from normalized type_ids",
                    ));
                }
                for id in values.values() {
                    let runtime = id & ((1 << 30) | (1 << 31)) != 0;
                    if !runtime && !entity_ids.contains(id) {
                        return Err(corrupt(
                            "semantic topology contains an unbound ontology entity id",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate every routed Parquet file against authenticated schema metadata and join keys.
    pub fn validate_physical_routes(&self, graph_root: &Path) -> Result<(), GfError> {
        self.validate_physical_routes_with_inventory(graph_root, None)
    }

    /// Validate routed files against the authenticated generation inventory.
    /// Callers opening a committed generation must provide its `graph/files`
    /// record; directory enumeration alone is not publication authority.
    pub fn validate_physical_routes_with_inventory(
        &self,
        graph_root: &Path,
        inventory: Option<&crate::GraphFilesInventory>,
    ) -> Result<(), GfError> {
        let expected = self
            .bindings
            .iter()
            .filter_map(|binding| binding.physical_path(graph_root))
            .collect::<BTreeSet<_>>();
        let inventory_paths = inventory.map(|inventory| {
            inventory
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str())
                .collect::<BTreeSet<_>>()
        });
        for subdir in ["topology/edges", "properties", "edge_properties"] {
            let directory = graph_root.join(subdir);
            let Ok(entries) = std::fs::read_dir(&directory) else {
                continue;
            };
            for entry in entries {
                let path = entry
                    .map_err(|_| corrupt("semantic route inventory cannot be read"))?
                    .path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("s-") && name.ends_with(".parquet"))
                    && !expected.contains(&path)
                {
                    return Err(corrupt("unlisted opaque semantic route file is present"));
                }
            }
        }
        for binding in &self.bindings {
            let Some(path) = binding.physical_path(graph_root) else {
                continue;
            };
            let relative = path
                .strip_prefix(graph_root)
                .map_err(|_| corrupt("semantic route escapes graph inventory"))?
                .to_string_lossy()
                .replace('\\', "/");
            if path.exists()
                && inventory_paths
                    .as_ref()
                    .is_some_and(|paths| !paths.contains(relative.as_str()))
            {
                return Err(corrupt(
                    "semantic route is absent from authenticated graph inventory",
                ));
            }
            if !path.exists() {
                continue;
            }
            preflight_parquet_footer(&path)?;
            let file = File::open(&path).map_err(|_| corrupt("semantic route file is missing"))?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|_| corrupt("semantic route Parquet metadata is invalid"))?;
            let schema = builder.schema();
            if schema.fields().len() > MAX_SEMANTIC_PARQUET_COLUMNS {
                return Err(corrupt("semantic route column count exceeds limit"));
            }
            if schema.metadata().get(SEMANTIC_ROUTE_METADATA_KEY) != Some(&binding.route)
                || schema.metadata().get(SEMANTIC_COMPOSITION_METADATA_KEY)
                    != Some(&self.composition_fingerprint)
            {
                return Err(corrupt(&format!(
                    "semantic route metadata does not match binding for {relative}: expected route {} and composition {}, found route {:?} and composition {:?}",
                    binding.route,
                    self.composition_fingerprint,
                    schema.metadata().get(SEMANTIC_ROUTE_METADATA_KEY),
                    schema.metadata().get(SEMANTIC_COMPOSITION_METADATA_KEY),
                )));
            }
            let join_key = match binding.route_kind {
                SemanticRouteKind::NodeProperty => "node_uuid",
                SemanticRouteKind::Relation | SemanticRouteKind::EdgeProperty => "edge_uuid",
                SemanticRouteKind::Entity => unreachable!(),
            };
            let join_field = schema
                .field_with_name(join_key)
                .map_err(|_| corrupt("semantic route join key is missing"))?;
            if join_field.is_nullable()
                || join_field.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16)
            {
                return Err(corrupt("semantic route join key is missing"));
            }
        }
        self.validate_topology_ids(graph_root)?;
        Ok(())
    }

    /// Decode exact canonical JSON and fail closed on corruption or excess work.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        if bytes.len() > MAX_SEMANTIC_BINDING_BYTES {
            return Err(corrupt("semantic binding bytes exceed limit"));
        }
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| corrupt("semantic bindings are malformed"))?;
        value.validate()?;
        if value.to_canonical_json()? != bytes {
            return Err(corrupt("semantic bindings are not canonical"));
        }
        Ok(value)
    }

    /// Encode canonical JSON plus LF.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        self.validate()?;
        let mut bytes =
            serde_json::to_vec(self).map_err(|_| corrupt("semantic bindings cannot be encoded"))?;
        bytes.push(b'\n');
        if bytes.len() > MAX_SEMANTIC_BINDING_BYTES {
            return Err(corrupt("semantic binding bytes exceed limit"));
        }
        Ok(bytes)
    }

    /// Encode as registered graph participant.
    pub fn to_project_participant(&self) -> Result<ProjectParticipant, GfError> {
        let bytes = self.to_canonical_json()?;
        Ok(ProjectParticipant {
            capability_id: GRAPH_CAPABILITY_ID.into(),
            capability_version: crate::GRAPH_CAPABILITY_VERSION,
            record_family_id: GRAPH_SEMANTIC_BINDINGS_FAMILY.into(),
            record_version: GRAPH_SEMANTIC_BINDINGS_VERSION,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: Sha256::digest(b"graphforge-semantic-storage-bindings/1").into(),
            row_count: self.bindings.len() as u64,
            bytes,
        })
    }

    fn validate(&self) -> Result<(), GfError> {
        if self.contract_version != GRAPH_SEMANTIC_BINDINGS_VERSION
            || self.composition_fingerprint.len() != 64
            || !self
                .composition_fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(corrupt(
                "semantic binding contract or fingerprint is invalid",
            ));
        }
        if self.bindings.len() > MAX_SEMANTIC_BINDINGS {
            return Err(corrupt("semantic binding count exceeds limit"));
        }
        if self
            .bindings
            .windows(2)
            .any(|w| binding_key(&w[0]) >= binding_key(&w[1]))
        {
            return Err(corrupt(
                "semantic bindings are not strictly ordered and unique",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut routes = BTreeMap::new();
        for binding in &self.bindings {
            if binding.symbol.local_id.len() > MAX_SEMANTIC_STRING_BYTES
                || binding.symbol.module.ontology_id.len() > MAX_SEMANTIC_STRING_BYTES
                || binding.symbol.module.authored_version.len() > MAX_SEMANTIC_STRING_BYTES
            {
                return Err(corrupt("semantic binding string exceeds limit"));
            }
            if binding.route
                != Self::opaque_route(binding.route_kind, &binding.symbol, binding.owner.as_ref())
            {
                return Err(corrupt("semantic binding route is not authenticated"));
            }
            match binding.route_kind {
                SemanticRouteKind::Entity
                    if binding.owner.is_none() && binding.symbol.kind == SymbolKind::Entity => {}
                SemanticRouteKind::Relation
                    if binding.owner.is_none() && binding.symbol.kind == SymbolKind::Relation => {}
                SemanticRouteKind::NodeProperty | SemanticRouteKind::EdgeProperty
                    if binding.owner.is_some() && binding.symbol.kind == SymbolKind::Property => {}
                _ => {
                    return Err(corrupt(
                        "semantic binding kind, id, owner, or symbol disagrees",
                    ));
                }
            }
            if !ids.insert((id_namespace(binding.route_kind), binding.storage_id)) {
                return Err(corrupt("semantic storage id is reused"));
            }
            let route_owner = binding.owner.as_ref().unwrap_or(&binding.symbol).display();
            if routes
                .insert(
                    (binding.route_kind, binding.route.clone()),
                    route_owner.clone(),
                )
                .is_some_and(|prior| prior != route_owner)
            {
                return Err(corrupt("semantic route is reused by a different owner"));
            }
        }
        Ok(())
    }
}

impl SemanticStorageBinding {
    /// Exact physical path for routed data, or none for numeric entity bindings.
    #[must_use]
    pub fn physical_path(&self, root: &Path) -> Option<PathBuf> {
        match self.route_kind {
            SemanticRouteKind::Entity => None,
            SemanticRouteKind::Relation => Some(
                root.join("topology/edges")
                    .join(format!("{}.parquet", self.route)),
            ),
            SemanticRouteKind::NodeProperty => Some(
                root.join("properties")
                    .join(format!("{}.parquet", self.route)),
            ),
            SemanticRouteKind::EdgeProperty => Some(
                root.join("edge_properties")
                    .join(format!("{}.parquet", self.route)),
            ),
        }
    }
}

/// Load and validate the optional mapping pinned to one resolved generation.
pub fn semantic_storage_bindings(
    generation: &crate::ResolvedProjectGeneration,
) -> Result<Option<SemanticStorageBindings>, GfError> {
    let bindings = generation
        .participant_snapshot(GRAPH_CAPABILITY_ID, GRAPH_SEMANTIC_BINDINGS_FAMILY)?
        .map(|snapshot| {
            let expected_schema: [u8; 32] =
                Sha256::digest(b"graphforge-semantic-storage-bindings/1").into();
            if snapshot.capability_version != crate::GRAPH_CAPABILITY_VERSION
                || snapshot.record_version != GRAPH_SEMANTIC_BINDINGS_VERSION
                || snapshot.encoding != "json"
                || snapshot.schema_fingerprint != expected_schema
            {
                return Err(corrupt(
                    "semantic binding participant descriptor is unsupported",
                ));
            }
            let bindings = SemanticStorageBindings::from_canonical_json(&snapshot.bytes)?;
            if snapshot.row_count != bindings.bindings.len() as u64 {
                return Err(corrupt("semantic binding participant row count disagrees"));
            }
            Ok(bindings)
        })
        .transpose()?;
    if let Some(bindings) = &bindings {
        let composition = generation
            .participant_snapshot("workspace", "ontology_composition")?
            .ok_or_else(|| corrupt("semantic bindings have no persisted composition authority"))?;
        if composition.bytes.len() > MAX_SEMANTIC_BINDING_BYTES {
            return Err(corrupt(
                "persisted composition authority exceeds validation limit",
            ));
        }
        let value: serde_json::Value = serde_json::from_slice(&composition.bytes)
            .map_err(|_| corrupt("persisted composition authority is malformed"))?;
        if value
            .get("composition_fingerprint")
            .and_then(serde_json::Value::as_str)
            != Some(bindings.composition_fingerprint.as_str())
        {
            return Err(corrupt(
                "semantic bindings and persisted composition fingerprints disagree",
            ));
        }
    }
    Ok(bindings)
}

/// Refuse a data-bearing legacy semantic graph until the composition lifecycle
/// stages its deterministic route rewrite and binding participant together.
/// Empty graphs remain bootstrap-compatible.
pub fn require_atomic_legacy_migration(graph_root: &Path) -> Result<(), GfError> {
    let mut inspected = 0usize;
    for subdir in [
        "topology",
        "topology/edges",
        "properties",
        "edge_properties",
    ] {
        let directory = graph_root.join(subdir);
        let Ok(entries) = std::fs::read_dir(directory) else {
            continue;
        };
        for entry in entries {
            inspected = inspected
                .checked_add(1)
                .ok_or_else(|| corrupt("legacy migration inventory overflows"))?;
            if inspected > MAX_SEMANTIC_BINDINGS {
                return Err(corrupt("legacy migration inventory exceeds limit"));
            }
            let path = entry
                .map_err(|_| corrupt("legacy migration inventory cannot be read"))?
                .path();
            if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
                continue;
            }
            preflight_parquet_footer(&path)?;
            let metadata = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(&path)
                    .map_err(|_| corrupt("legacy migration Parquet cannot be opened"))?,
            )
            .map_err(|_| corrupt("legacy migration Parquet metadata is invalid"))?
            .metadata()
            .file_metadata()
            .num_rows();
            if metadata > 0 {
                return Err(GfError::Validation(
                    "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: data-bearing legacy routes must be rewritten and published with semantic bindings as one staged generation".into(),
                ));
            }
        }
    }
    Ok(())
}

/// Rewrite a preflighted unambiguous legacy workspace to authenticated opaque
/// routes. The caller must hold graph publication authority and publish the
/// returned binding participant in the same generation. A local failure rolls
/// every completed rename back before returning.
pub struct LegacyRouteMigration {
    completed: Vec<(PathBuf, PathBuf, PathBuf)>,
    committed: bool,
}

impl LegacyRouteMigration {
    /// Keep the opaque routes after their binding participant is published.
    pub fn commit(&mut self) {
        for (_, _, backup) in &self.completed {
            let _ = std::fs::remove_file(backup);
        }
        self.committed = true;
    }
}

impl Drop for LegacyRouteMigration {
    fn drop(&mut self) {
        if !self.committed {
            for (old, new, backup) in self.completed.iter().rev() {
                let _ = std::fs::remove_file(new);
                let _ = std::fs::rename(backup, old);
            }
        }
    }
}

/// Apply deterministic preflighted route moves and return a rollback guard.
/// The caller commits the guard only after publishing the matching bindings.
pub fn apply_legacy_route_moves(
    graph_root: &Path,
    route_moves: &[(PathBuf, PathBuf)],
    bindings: &SemanticStorageBindings,
) -> Result<LegacyRouteMigration, GfError> {
    let mut completed = Vec::new();
    for (old_relative, new_relative) in route_moves {
        let old = graph_root.join(old_relative);
        let new = graph_root.join(new_relative);
        let binding = bindings
            .bindings
            .iter()
            .find(|binding| binding.physical_path(Path::new("")).as_ref() == Some(new_relative))
            .ok_or_else(|| corrupt("legacy migration destination has no binding"))?;
        if new.exists() {
            return Err(corrupt("legacy migration destination already exists"));
        }
        let parent = new
            .parent()
            .ok_or_else(|| corrupt("legacy migration destination has no parent"))?;
        std::fs::create_dir_all(parent)
            .map_err(|_| corrupt("legacy migration destination cannot be created"))?;
        let backup = old.with_extension("parquet.legacy-backup");
        if backup.exists() {
            return Err(corrupt("legacy migration backup already exists"));
        }
        if let Err(error) = std::fs::rename(&old, &backup) {
            for (prior_old, prior_new, prior_backup) in completed.iter().rev() {
                let _ = std::fs::remove_file(prior_new);
                let _ = std::fs::rename(prior_backup, prior_old);
            }
            return Err(corrupt(&format!("legacy migration rename failed: {error}")));
        }
        let rewrite = (|| {
            preflight_parquet_footer(&backup)?;
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(&backup).map_err(|_| corrupt("legacy migration source cannot open"))?,
            )
            .map_err(|_| corrupt("legacy migration source metadata is invalid"))?;
            if builder.schema().fields().len() > MAX_SEMANTIC_PARQUET_COLUMNS {
                return Err(corrupt("legacy migration column count exceeds limit"));
            }
            let mut metadata = builder.schema().metadata().clone();
            metadata.insert(SEMANTIC_ROUTE_METADATA_KEY.into(), binding.route.clone());
            metadata.insert(
                SEMANTIC_COMPOSITION_METADATA_KEY.into(),
                bindings.composition_fingerprint.clone(),
            );
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
                builder.schema().fields().clone(),
                metadata,
            ));
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&new)
                    .map_err(|_| corrupt("legacy migration destination cannot open"))?,
                schema,
                None,
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
        })();
        if let Err(error) = rewrite {
            let _ = std::fs::remove_file(&new);
            let _ = std::fs::rename(&backup, &old);
            for (prior_old, prior_new, prior_backup) in completed.iter().rev() {
                let _ = std::fs::remove_file(prior_new);
                let _ = std::fs::rename(prior_backup, prior_old);
            }
            return Err(error);
        }
        completed.push((old, new, backup));
    }
    Ok(LegacyRouteMigration {
        completed,
        committed: false,
    })
}

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
        let mut files_materialized = 0_u64;
        let mut rows_rewritten = 0_u64;
        let mut max_batch_rows = 0_usize;
        for entry in &inventory.files {
            checkpoint()?;
            let source = source_graph_root.join(&entry.relative_path);
            let relative = PathBuf::from(&entry.relative_path);
            let old_route = source
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|stem| stem.starts_with("s-"));
            let target_relative = if let Some(old_route) = old_route {
                if let Some(new_route) = route_moves.get(old_route) {
                    relative.with_file_name(format!("{new_route}.parquet"))
                } else {
                    relative.clone()
                }
            } else {
                relative.clone()
            };
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
            preflight_parquet_footer(&source)?;
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(&source).map_err(|_| corrupt("migration Parquet cannot be opened"))?,
            )
            .map_err(|_| corrupt("migration Parquet metadata is invalid"))?;
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
            metadata.insert(
                SEMANTIC_COMPOSITION_METADATA_KEY.into(),
                plan.to_composition_fingerprint.clone(),
            );
            if let Some(route) = new_route {
                metadata.insert(SEMANTIC_ROUTE_METADATA_KEY.into(), route);
            }
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
                fields, metadata,
            ));
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                File::create(&target).map_err(|_| corrupt("migration output cannot be opened"))?,
                schema.clone(),
                None,
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
            let path = binding
                .physical_path(&staging_graph_root)
                .ok_or_else(|| corrupt("migration target property route is absent"))?;
            if !path.exists() {
                continue;
            }
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                File::open(path)
                    .map_err(|_| corrupt("migration target property table cannot be opened"))?,
            )
            .map_err(|_| corrupt("migration target property schema is invalid"))?;
            let property_name = property
                .symbol
                .local_id
                .split_once(':')
                .map(|(_, name)| name)
                .ok_or_else(|| corrupt("migration target property identity is malformed"))?;
            if let Ok(field) = builder.schema().field_with_name(property_name)
                && (format!("{:?}", field.data_type()) != property.arrow_data_type
                    || field.is_nullable() != property.nullable)
            {
                return Err(corrupt(
                    "migration target property column type or nullability disagrees",
                ));
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
        preflight_parquet_footer(&path)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).map_err(|_| corrupt("migration impact Parquet cannot be opened"))?,
        )
        .map_err(|_| corrupt("migration impact Parquet metadata is invalid"))?
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

fn corrupt(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn preflight_parquet_footer(path: &Path) -> Result<(), GfError> {
    let mut file = File::open(path).map_err(|_| corrupt("semantic Parquet cannot be opened"))?;
    let length = file
        .metadata()
        .map_err(|_| corrupt("semantic Parquet metadata cannot be read"))?
        .len();
    if length < 12 {
        return Err(corrupt("semantic Parquet footer is truncated"));
    }
    file.seek(SeekFrom::End(-8))
        .map_err(|_| corrupt("semantic Parquet footer cannot be read"))?;
    let mut footer = [0_u8; 8];
    file.read_exact(&mut footer)
        .map_err(|_| corrupt("semantic Parquet footer cannot be read"))?;
    if &footer[4..] != b"PAR1" {
        return Err(corrupt("semantic Parquet footer magic is invalid"));
    }
    let metadata_len = u64::from(u32::from_le_bytes(footer[..4].try_into().unwrap()));
    if metadata_len > MAX_SEMANTIC_PARQUET_METADATA_BYTES
        || metadata_len
            .checked_add(8)
            .is_none_or(|total| total > length)
    {
        return Err(corrupt("semantic Parquet metadata exceeds admission limit"));
    }
    Ok(())
}

fn legacy_ambiguous(message: &str) -> GfError {
    GfError::Validation(format!(
        "GF_SEMANTIC_LEGACY_AMBIGUOUS: {message}; qualify module ownership or migrate explicitly"
    ))
}

fn qualified(
    module: &graphforge_ontology::OntologyModuleId,
    kind: SymbolKind,
    local_id: &str,
) -> QualifiedSymbol {
    QualifiedSymbol {
        module: module.clone(),
        kind,
        local_id: local_id.to_owned(),
    }
}

fn binding(
    route_kind: SemanticRouteKind,
    storage_id: u32,
    symbol: QualifiedSymbol,
    owner: Option<QualifiedSymbol>,
) -> SemanticStorageBinding {
    SemanticStorageBinding {
        route_kind,
        storage_id,
        route: SemanticStorageBindings::opaque_route(route_kind, &symbol, owner.as_ref()),
        symbol,
        owner,
    }
}

fn binding_has_retained_data(
    binding: &SemanticStorageBinding,
    graph_root: &Path,
) -> Result<bool, GfError> {
    if binding.route_kind == SemanticRouteKind::Entity {
        use arrow::array::{Array, ListArray, UInt32Array};
        let path = graph_root.join("topology/nodes.parquet");
        if !path.exists() {
            return Ok(false);
        }
        preflight_parquet_footer(&path)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).map_err(|_| corrupt("removal topology cannot be opened"))?,
        )
        .map_err(|_| corrupt("removal topology metadata is invalid"))?
        .with_batch_size(8192)
        .build()
        .map_err(|_| corrupt("removal topology reader cannot be built"))?;
        for batch in reader {
            let batch = batch.map_err(|_| corrupt("removal topology batch is invalid"))?;
            let values = batch
                .column_by_name("type_ids")
                .and_then(|array| array.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| corrupt("removal topology type_ids is malformed"))?;
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
        }
        return Ok(false);
    }
    let Some(path) = binding.physical_path(graph_root) else {
        return Ok(false);
    };
    if !path.exists() {
        return Ok(false);
    }
    preflight_parquet_footer(&path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|_| corrupt("removal route cannot be opened"))?,
    )
    .map_err(|_| corrupt("removal route metadata is invalid"))?;
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
        if !builder
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == column)
        {
            return Ok(false);
        }
        for batch in builder
            .with_batch_size(8192)
            .build()
            .map_err(|_| corrupt("removal property reader cannot be built"))?
        {
            let batch = batch.map_err(|_| corrupt("removal property batch is invalid"))?;
            let values = batch
                .column_by_name(column)
                .ok_or_else(|| corrupt("removal property column disappeared"))?;
            if values.null_count() < values.len() {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    Ok(builder.metadata().file_metadata().num_rows() > 0)
}

fn binding_key(
    binding: &SemanticStorageBinding,
) -> (SemanticRouteKind, u32, String, String, String) {
    (
        binding.route_kind,
        binding.storage_id,
        binding.route.clone(),
        binding.symbol.display(),
        binding
            .owner
            .as_ref()
            .map_or_else(String::new, QualifiedSymbol::display),
    )
}

fn projection_key(
    kind: SemanticRouteKind,
    symbol: &QualifiedSymbol,
    owner: Option<&QualifiedSymbol>,
) -> (SemanticRouteKind, String, String) {
    (
        kind,
        symbol.display(),
        owner.map_or_else(String::new, QualifiedSymbol::display),
    )
}

fn lineage_key(
    kind: SemanticRouteKind,
    symbol: &QualifiedSymbol,
    owner: Option<&QualifiedSymbol>,
) -> (SemanticRouteKind, String, String, String) {
    (
        kind,
        symbol.module.ontology_id.clone(),
        symbol.local_id.clone(),
        owner.map_or_else(String::new, |owner| owner.local_id.clone()),
    )
}

const fn route_kind_token(kind: SemanticRouteKind) -> &'static str {
    match kind {
        SemanticRouteKind::Entity | SemanticRouteKind::NodeProperty => "entity_owner",
        SemanticRouteKind::Relation | SemanticRouteKind::EdgeProperty => "relation_owner",
    }
}

const fn id_namespace(kind: SemanticRouteKind) -> u8 {
    match kind {
        SemanticRouteKind::Entity => 0,
        SemanticRouteKind::Relation => 1,
        SemanticRouteKind::NodeProperty | SemanticRouteKind::EdgeProperty => 2,
    }
}
fn hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_ontology::{
        ActivationMode, AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
        MigrationDef, OntologyDoc, OntologyModuleId, PropertyDef, PropertyValueType,
        RelationTypeDef, SemanticFlags, compile_inventory, module_document_digest,
    };
    fn module(name: &str) -> OntologyModuleId {
        OntologyModuleId {
            ontology_id: name.into(),
            authored_version: "1".into(),
            canonical_digest: "0".repeat(64),
        }
    }
    fn symbol(module_name: &str, kind: SymbolKind, local_id: &str) -> QualifiedSymbol {
        QualifiedSymbol {
            module: module(module_name),
            kind,
            local_id: local_id.into(),
        }
    }
    #[test]
    fn collisions_have_distinct_routes_and_reopen_exactly() {
        let a = symbol("a", SymbolKind::Entity, "Person");
        let b = symbol("b", SymbolKind::Entity, "Person");
        let values = vec![a, b]
            .into_iter()
            .enumerate()
            .map(|(id, symbol)| SemanticStorageBinding {
                route_kind: SemanticRouteKind::Entity,
                storage_id: id as u32,
                route: SemanticStorageBindings::opaque_route(
                    SemanticRouteKind::Entity,
                    &symbol,
                    None,
                ),
                symbol,
                owner: None,
            })
            .collect();
        let record = SemanticStorageBindings::new("1".repeat(64), values).unwrap();
        assert_ne!(record.bindings[0].route, record.bindings[1].route);
        assert_eq!(
            SemanticStorageBindings::from_canonical_json(&record.to_canonical_json().unwrap())
                .unwrap(),
            record
        );
    }
    #[test]
    fn corruption_and_limits_fail_before_acceptance() {
        let symbol = symbol("a", SymbolKind::Relation, "KNOWS");
        let mut binding = SemanticStorageBinding {
            route_kind: SemanticRouteKind::Relation,
            storage_id: 1,
            route: SemanticStorageBindings::opaque_route(
                SemanticRouteKind::Relation,
                &symbol,
                None,
            ),
            symbol,
            owner: None,
        };
        binding.route.push('x');
        assert!(SemanticStorageBindings::new("1".repeat(64), vec![binding]).is_err());
        assert!(
            SemanticStorageBindings::from_canonical_json(&vec![
                b' ';
                MAX_SEMANTIC_BINDING_BYTES + 1
            ])
            .is_err()
        );
    }

    fn compiled_with(
        version: &str,
        declare_migration: bool,
        include_edge_property: bool,
    ) -> CompiledComposition {
        let doc = OntologyDoc {
            ontology_id: "https://example.test/core".into(),
            version: version.into(),
            entity_types: vec![EntityTypeDef {
                name: "Person".into(),
                r#abstract: false,
                parent: None,
            }],
            relation_types: vec![RelationTypeDef {
                name: "KNOWS".into(),
                src: "Person".into(),
                dst: "Person".into(),
                inverse: None,
                semantic: SemanticFlags::default(),
            }],
            properties: vec![
                PropertyDef {
                    owner: "Person".into(),
                    name: "name".into(),
                    value_type: PropertyValueType::Utf8,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
                PropertyDef {
                    owner: "Person".into(),
                    name: "birth_year".into(),
                    value_type: PropertyValueType::Int64,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
                PropertyDef {
                    owner: "KNOWS".into(),
                    name: "since".into(),
                    value_type: PropertyValueType::Int64,
                    nullable: true,
                    multivalued: false,
                    default_json: None,
                },
            ]
            .into_iter()
            .filter(|property| include_edge_property || property.name != "since")
            .collect(),
            constraints: vec![],
            migrations: (declare_migration && version != "1")
                .then_some(MigrationDef {
                    from_version: "1".into(),
                    to_version: version.into(),
                    transform_kind: "identity".into(),
                    script_ref: None,
                    checksum: None,
                })
                .into_iter()
                .collect(),
        };
        let module = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: version.into(),
                canonical_digest: module_document_digest(&doc).unwrap(),
            },
            dependencies: vec![],
            doc,
            allow_projected_identity: false,
        };
        compile_inventory(InventoryCompileRequest {
            modules: &[module],
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap()
    }

    fn compiled(version: &str) -> CompiledComposition {
        compiled_with(version, true, true)
    }

    fn renamed_compiled() -> CompiledComposition {
        let mut doc = compiled_with("1", false, true).modules.remove(0).doc;
        doc.version = "2".into();
        doc.entity_types[0].name = "Human".into();
        doc.relation_types[0].src = "Human".into();
        doc.relation_types[0].dst = "Human".into();
        for property in &mut doc.properties {
            if property.owner == "Person" {
                property.owner = "Human".into();
            }
        }
        doc.properties[0].name = "display_name".into();
        doc.migrations = vec![
            MigrationDef {
                from_version: "1".into(),
                to_version: "1.5".into(),
                transform_kind: "rename_type:Person->Human".into(),
                script_ref: None,
                checksum: None,
            },
            MigrationDef {
                from_version: "1.5".into(),
                to_version: "2".into(),
                transform_kind: "rename_property:Human|name->display_name".into(),
                script_ref: None,
                checksum: None,
            },
        ];
        let authored = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: doc.version.clone(),
                canonical_digest: module_document_digest(&doc).unwrap(),
            },
            dependencies: vec![],
            doc,
            allow_projected_identity: false,
        };
        compile_inventory(InventoryCompileRequest {
            modules: &[authored],
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap()
    }

    #[test]
    fn retained_entity_and_property_rename_materializes_deterministically() {
        use std::collections::HashMap;

        let old = compiled_with("1", false, true);
        let old_bindings = SemanticStorageBindings::project(&old, None).unwrap();
        let entity = old_bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let source = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(source.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(old.fingerprint.clone()));
        let node = graphforge_core::uuid::new_v7();
        writer
            .create_node(node, graphforge_core::TypeId(entity.storage_id))
            .unwrap();
        writer
            .set_properties(
                &node,
                Some(&entity.route),
                HashMap::from([
                    ("name".into(), graphforge_ir::IrLiteral::Str("Ada".into())),
                    ("birth_year".into(), graphforge_ir::IrLiteral::Int(1815)),
                ]),
            )
            .unwrap();
        writer.flush().unwrap();

        let next = renamed_compiled();
        let first = SemanticStorageBindings::plan_retained_data_migration(
            &old,
            &next,
            &old_bindings,
            source.path(),
        )
        .unwrap();
        let second = SemanticStorageBindings::plan_retained_data_migration(
            &old,
            &next,
            &old_bindings,
            source.path(),
        )
        .unwrap();
        assert_eq!(first, second);
        assert!(first.retained_rows_scanned > 0);
        let renamed_entity = first
            .bindings
            .bindings
            .iter()
            .find(|binding| binding.symbol.local_id == "Human")
            .unwrap();
        assert_eq!(renamed_entity.storage_id, entity.storage_id);

        let parent = tempfile::TempDir::new().unwrap();
        let candidate = parent.path().join("candidate");
        let evidence = materialize_semantic_migration(
            &first,
            source.path(),
            &candidate,
            SemanticMigrationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(evidence.plan_digest, first.plan_digest);
        first.bindings.validate_physical_routes(&candidate).unwrap();
        let path = candidate
            .join("properties")
            .join(format!("{}.parquet", renamed_entity.route));
        let schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).unwrap(),
        )
        .unwrap()
        .schema()
        .clone();
        assert!(schema.field_with_name("display_name").is_ok());
        assert!(schema.field_with_name("birth_year").is_ok());
        assert!(schema.field_with_name("name").is_err());
        let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(
                candidate
                    .join("properties")
                    .join(format!("{}.parquet", renamed_entity.route)),
            )
            .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let birth_year = batch
            .column_by_name("birth_year")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(birth_year.value(0), 1815);
    }

    #[test]
    fn cancelled_materialization_removes_private_candidate() {
        let old = compiled_with("1", false, true);
        let bindings = SemanticStorageBindings::project(&old, None).unwrap();
        let source = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(source.path().join("catalog")).unwrap();
        std::fs::write(source.path().join("catalog/state.json"), b"{}\n").unwrap();
        let next = renamed_compiled();
        let plan = SemanticStorageBindings::plan_retained_data_migration(
            &old,
            &next,
            &bindings,
            source.path(),
        )
        .unwrap();
        let parent = tempfile::TempDir::new().unwrap();
        let candidate = parent.path().join("candidate");
        let mut checkpoints = 0;
        let error = materialize_semantic_migration(
            &plan,
            source.path(),
            &candidate,
            SemanticMigrationLimits::default(),
            || {
                checkpoints += 1;
                if checkpoints == 2 {
                    Err(GfError::Validation("cancelled".into()))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(!candidate.exists());
        assert!(std::fs::read_dir(parent.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".semantic-migration-")
        }));
    }

    #[test]
    fn materializer_rejects_graph_inventory_drift_from_preview() {
        let old = compiled_with("1", false, true);
        let bindings = SemanticStorageBindings::project(&old, None).unwrap();
        let source = tempfile::TempDir::new().unwrap();
        let next = renamed_compiled();
        let plan = SemanticStorageBindings::plan_retained_data_migration(
            &old,
            &next,
            &bindings,
            source.path(),
        )
        .unwrap();
        std::fs::create_dir_all(source.path().join("catalog")).unwrap();
        std::fs::write(source.path().join("catalog/drift.json"), b"{}\n").unwrap();
        let parent = tempfile::TempDir::new().unwrap();
        let candidate = parent.path().join("candidate");
        let error = materialize_semantic_migration(
            &plan,
            source.path(),
            &candidate,
            SemanticMigrationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("differs from the planned"));
        assert!(!candidate.exists());
    }

    #[test]
    fn non_null_property_addition_rejects_retained_owner_without_backfill() {
        let old = compiled_with("1", false, true);
        let bindings = SemanticStorageBindings::project(&old, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let source = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(source.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(old.fingerprint.clone()));
        writer
            .create_node(
                graphforge_core::uuid::new_v7(),
                graphforge_core::TypeId(entity.storage_id),
            )
            .unwrap();
        writer.flush().unwrap();

        let mut doc = compiled_with("1", false, true).modules.remove(0).doc;
        doc.version = "2".into();
        doc.properties.push(PropertyDef {
            owner: "Person".into(),
            name: "required_code".into(),
            value_type: PropertyValueType::Utf8,
            nullable: false,
            multivalued: false,
            default_json: Some("\"unknown\"".into()),
        });
        doc.migrations.push(MigrationDef {
            from_version: "1".into(),
            to_version: "2".into(),
            transform_kind: "add_property:Person|required_code|utf8|false".into(),
            script_ref: None,
            checksum: None,
        });
        let authored = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: doc.ontology_id.clone(),
                authored_version: doc.version.clone(),
                canonical_digest: module_document_digest(&doc).unwrap(),
            },
            dependencies: vec![],
            doc,
            allow_projected_identity: false,
        };
        let next = compile_inventory(InventoryCompileRequest {
            modules: &[authored],
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        let error = SemanticStorageBindings::plan_retained_data_migration(
            &old,
            &next,
            &bindings,
            source.path(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("typed retained-data backfill"));
    }

    #[test]
    fn owner_routes_match_write_routing_and_ids_carry_across_module_upgrade() {
        let first = compiled("1");
        let initial = SemanticStorageBindings::project(&first, None).unwrap();
        let entity = initial
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let node_property = initial
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::NodeProperty)
            .unwrap();
        let relation = initial
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Relation)
            .unwrap();
        let edge_property = initial
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::EdgeProperty)
            .unwrap();
        assert_eq!(entity.route, node_property.route);
        assert_eq!(relation.route, edge_property.route);

        let upgraded = compiled("2");
        let carried = SemanticStorageBindings::project(&upgraded, Some(&initial)).unwrap();
        for binding in &carried.bindings {
            let old = initial
                .bindings
                .iter()
                .find(|old| {
                    old.route_kind == binding.route_kind
                        && old.symbol.local_id == binding.symbol.local_id
                })
                .unwrap();
            assert_eq!(binding.storage_id, old.storage_id);
        }

        assert_eq!(
            SemanticStorageBindings::project(&upgraded, Some(&carried)).unwrap(),
            carried,
            "reprojecting the same generation must be idempotent"
        );
        let undeclared = compiled_with("3", false, true);
        assert!(SemanticStorageBindings::project(&undeclared, Some(&carried)).is_err());
    }

    #[test]
    fn removal_requires_a_pinned_scan_and_refuses_retained_property_data() {
        use std::collections::HashMap;

        let first = compiled("1");
        let initial = SemanticStorageBindings::project(&first, None).unwrap();
        let relation = initial
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Relation)
            .unwrap();
        let next = compiled_with("2", true, false);
        assert!(SemanticStorageBindings::project(&next, Some(&initial)).is_err());

        let empty = tempfile::TempDir::new().unwrap();
        let removed =
            SemanticStorageBindings::project_with_graph_scan(&next, Some(&initial), empty.path())
                .unwrap();
        assert!(!removed.bindings.iter().any(|binding| {
            binding.route_kind == SemanticRouteKind::EdgeProperty
                && binding.symbol.local_id == "KNOWS:since"
        }));

        let other_column = tempfile::TempDir::new().unwrap();
        let mut writer = crate::GraphWriter::open_at(
            other_column.path(),
            graphforge_core::OntologyMode::Strict,
            1,
        )
        .unwrap()
        .with_semantic_composition_fingerprint(Some(first.fingerprint.clone()));
        let left = graphforge_core::uuid::new_v7();
        let right = graphforge_core::uuid::new_v7();
        writer
            .create_node(left, graphforge_core::TypeId(1))
            .unwrap();
        writer
            .create_node(right, graphforge_core::TypeId(1))
            .unwrap();
        let edge = graphforge_core::uuid::new_v7();
        writer
            .create_edge(edge, &relation.route, &left, &right)
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some(&relation.route),
                HashMap::from([("other".into(), graphforge_ir::IrLiteral::Int(1))]),
            )
            .unwrap();
        writer.flush().unwrap();
        SemanticStorageBindings::project_with_graph_scan(
            &next,
            Some(&initial),
            other_column.path(),
        )
        .expect("an unrelated populated owner column must not retain the removed property");

        let retained = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(retained.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(first.fingerprint.clone()));
        let left = graphforge_core::uuid::new_v7();
        let right = graphforge_core::uuid::new_v7();
        writer
            .create_node(left, graphforge_core::TypeId(1))
            .unwrap();
        writer
            .create_node(right, graphforge_core::TypeId(1))
            .unwrap();
        let edge = graphforge_core::uuid::new_v7();
        writer
            .create_edge(edge, &relation.route, &left, &right)
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some(&relation.route),
                HashMap::from([("since".into(), graphforge_ir::IrLiteral::Int(2026))]),
            )
            .unwrap();
        writer.flush().unwrap();
        assert!(
            SemanticStorageBindings::project_with_graph_scan(
                &next,
                Some(&initial),
                retained.path(),
            )
            .is_err()
        );
    }

    #[test]
    fn legacy_scanner_resource_ladder_is_independent_of_total_rows() {
        let composition = compiled("1");
        for rows in [1_usize, 8_193] {
            let dir = tempfile::TempDir::new().unwrap();
            let mut writer =
                crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                    .unwrap();
            for _ in 0..rows {
                writer
                    .create_node(graphforge_core::uuid::new_v7(), graphforge_core::TypeId(0))
                    .unwrap();
            }
            writer.flush().unwrap();
            let projection =
                SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path())
                    .unwrap();
            assert_eq!(projection.topology_rows_scanned, rows as u64);
            assert!(projection.max_topology_batch_rows <= 8_192);
        }

        let mut ambiguous = composition;
        ambiguous.modules.extend(compiled("1").modules);
        let error = SemanticStorageBindings::project_legacy_unambiguous(
            &ambiguous,
            tempfile::TempDir::new().unwrap().path(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("GF_SEMANTIC_LEGACY_AMBIGUOUS"));
    }

    #[test]
    fn parquet_footer_limit_fails_before_decoder_allocation() {
        use std::io::Write;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("topology/nodes.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::create(&path).unwrap();
        file.write_all(b"PAR1").unwrap();
        file.write_all(&(u32::MAX).to_le_bytes()).unwrap();
        file.write_all(b"PAR1").unwrap();
        drop(file);
        let error = preflight_parquet_footer(&path).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("metadata exceeds admission limit"),
            "{error:?}"
        );
        let composition = compiled("1");
        let error = SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("metadata exceeds admission limit"),
            "{error:?}"
        );
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let error = bindings.validate_physical_routes(dir.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("metadata exceeds admission limit"),
            "{error:?}"
        );
        let error = require_atomic_legacy_migration(dir.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("metadata exceeds admission limit"),
            "{error:?}"
        );
    }

    #[test]
    fn unambiguous_legacy_routes_rewrite_with_metadata_and_reopen() {
        use std::collections::HashMap;

        let composition = compiled("1");
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap();
        let left = graphforge_core::uuid::new_v7();
        let right = graphforge_core::uuid::new_v7();
        writer
            .create_node(left, graphforge_core::TypeId(0))
            .unwrap();
        writer
            .create_node(right, graphforge_core::TypeId(0))
            .unwrap();
        let edge = graphforge_core::uuid::new_v7();
        writer.create_edge(edge, "KNOWS", &left, &right).unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some("KNOWS"),
                HashMap::from([("since".into(), graphforge_ir::IrLiteral::Int(2020))]),
            )
            .unwrap();
        writer.flush().unwrap();

        let projection =
            SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap();
        assert!(!projection.route_moves.is_empty());
        let mut migration =
            apply_legacy_route_moves(dir.path(), &projection.route_moves, &projection.bindings)
                .unwrap();
        projection
            .bindings
            .validate_physical_routes(dir.path())
            .unwrap();
        migration.commit();
        drop(migration);
        assert!(!dir.path().join("topology/edges/KNOWS.parquet").exists());
        projection
            .bindings
            .validate_physical_routes(dir.path())
            .unwrap();
    }

    #[test]
    fn normal_writer_authenticates_owner_routes_and_reopen_validation() {
        use std::collections::HashMap;

        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let relation = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Relation)
            .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(composition.fingerprint.clone()));
        let left = graphforge_core::uuid::new_v7();
        let right = graphforge_core::uuid::new_v7();
        writer
            .create_node(left, graphforge_core::TypeId(entity.storage_id))
            .unwrap();
        writer
            .create_node(right, graphforge_core::TypeId(entity.storage_id))
            .unwrap();
        writer
            .set_properties(
                &left,
                Some(&entity.route),
                HashMap::from([("name".into(), graphforge_ir::IrLiteral::Str("Ada".into()))]),
            )
            .unwrap();
        let edge = graphforge_core::uuid::new_v7();
        writer
            .create_edge(edge, &relation.route, &left, &right)
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some(&relation.route),
                HashMap::from([("since".into(), graphforge_ir::IrLiteral::Int(2026))]),
            )
            .unwrap();
        writer.flush().unwrap();
        bindings.validate_physical_routes(dir.path()).unwrap();
        let (inventory, _) = crate::capture_graph_files(dir.path()).unwrap();
        bindings
            .validate_physical_routes_with_inventory(dir.path(), Some(&inventory))
            .unwrap();
        let mut omitted = inventory.clone();
        omitted.files.retain(|entry| {
            entry.relative_path != format!("topology/edges/{}.parquet", relation.route)
        });
        omitted.file_count = omitted.files.len() as u64;
        assert!(
            bindings
                .validate_physical_routes_with_inventory(dir.path(), Some(&omitted))
                .is_err()
        );
        assert!(require_atomic_legacy_migration(dir.path()).is_err());

        let injected = dir.path().join("properties/s-deadbeef.parquet");
        std::fs::copy(
            dir.path()
                .join("properties")
                .join(format!("{}.parquet", entity.route)),
            injected,
        )
        .unwrap();
        assert!(bindings.validate_physical_routes(dir.path()).is_err());
    }
}
