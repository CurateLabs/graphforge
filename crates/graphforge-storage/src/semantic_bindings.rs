//! Generation-bound bindings between physical graph storage and qualified ontology authority.

mod migration;
pub use migration::materialize_semantic_migration;
mod legacy_routes;
pub use legacy_routes::{LegacyRouteMigration, apply_legacy_route_moves};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
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
            checked_semantic_storage_id(binding.storage_id)?;
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

        let entity_ids = self
            .bindings
            .iter()
            .filter(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .map(|binding| binding.storage_id)
            .collect::<BTreeSet<_>>();
        for path in crate::mutator::node_parquet_files(graph_root)? {
            let reader = admitted_semantic_parquet(&path)?
                .with_batch_size(8192)
                .build()
                .map_err(|_| corrupt("semantic topology reader cannot be built"))?;
            for batch in reader {
                let batch = batch.map_err(|_| corrupt("semantic topology batch is invalid"))?;
                let batch = crate::catalog::normalize_topology_nodes(vec![batch])
                    .map_err(|error| {
                        corrupt(&format!("semantic topology normalization failed: {error}"))
                    })?
                    .into_iter()
                    .next()
                    .ok_or_else(|| corrupt("semantic topology normalization disappeared"))?;
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
                    let values =
                        values
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .ok_or_else(|| {
                                corrupt("semantic topology type_ids has wrong element type")
                            })?;
                    let validate_entity = |id: graphforge_value::EntityTypeId| {
                        if let Some(declared) = id.tagged().ontology_id()
                            && !entity_ids.contains(&declared.0)
                        {
                            return Err(corrupt(
                                "semantic topology contains an unbound ontology entity id",
                            ));
                        }
                        Ok(())
                    };
                    // The primary is immutable property-routing authority, not
                    // a requirement on the node's current label membership.
                    let primary = graphforge_value::PrimaryEntityTypeId::decode(primary.value(row))
                        .map_err(|error| corrupt(&error.to_string()))?;
                    if let Some(id) = primary.label() {
                        validate_entity(id)?;
                    }
                    for raw in values {
                        let raw =
                            raw.ok_or_else(|| corrupt("semantic topology membership is null"))?;
                        let id = graphforge_value::EntityTypeId::decode(raw)
                            .map_err(|error| corrupt(&error.to_string()))?;
                        validate_entity(id)?;
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
    #[allow(
        clippy::too_many_lines,
        reason = "route and fragment authority checks stay atomic"
    )]
    pub fn validate_physical_routes_with_inventory(
        &self,
        graph_root: &Path,
        inventory: Option<&crate::GraphFilesInventory>,
    ) -> Result<(), GfError> {
        let captured = crate::capture_graph_files(graph_root)?.0;
        let inventory = match inventory {
            Some(inventory) => {
                if inventory != &captured {
                    return Err(corrupt(
                        "semantic route inventory disagrees with the graph tree",
                    ));
                }
                inventory
            }
            None => &captured,
        };
        let routes = semantic_fragment_inventory(graph_root, inventory)?;
        let expected = self
            .bindings
            .iter()
            .map(|binding| Ok::<_, GfError>(binding_fragments(binding, &routes)))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        let inventory_paths = inventory
            .files
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<BTreeSet<_>>();
        validate_no_unlisted_semantic_routes(graph_root, &expected)?;
        for ((_, route), paths) in &routes {
            if route.starts_with("s-") && paths.iter().any(|path| !expected.contains(path)) {
                return Err(corrupt("unlisted opaque semantic route file is present"));
            }
        }
        for binding in &self.bindings {
            for path in binding_fragments(binding, &routes) {
                validate_semantic_route_fragment(
                    binding,
                    &path,
                    graph_root,
                    Some(&inventory_paths),
                    &self.composition_fingerprint,
                )?;
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
            checked_semantic_storage_id(binding.storage_id)?;
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

type SemanticFragments = BTreeMap<(String, String), Vec<PathBuf>>;

fn semantic_fragment_inventory(
    root: &Path,
    inventory: &crate::GraphFilesInventory,
) -> Result<SemanticFragments, GfError> {
    let authority =
        crate::graph_projection::TransformRoutes::from_inventory(root, inventory.clone())?;
    let mut routes = BTreeMap::<(String, String), Vec<PathBuf>>::new();
    for entry in &inventory.files {
        let logical = authority.semantic_path(&entry.relative_path)?;
        if let Some(route) = semantic_route_from_wire(&logical) {
            let domain = if logical.starts_with("topology/edges/") {
                "topology/edges"
            } else if logical.starts_with("edge_properties/") {
                "edge_properties"
            } else {
                "properties"
            };
            let physical = crate::graph_files::resolve_v1_inventory_entry(root, entry)?;
            routes
                .entry((domain.to_owned(), route.to_owned()))
                .or_default()
                .push(physical);
        }
    }
    Ok(routes)
}

fn binding_fragments(binding: &SemanticStorageBinding, routes: &SemanticFragments) -> Vec<PathBuf> {
    let domain = match binding.route_kind {
        SemanticRouteKind::Entity => return Vec::new(),
        SemanticRouteKind::Relation => "topology/edges",
        SemanticRouteKind::NodeProperty => "properties",
        SemanticRouteKind::EdgeProperty => "edge_properties",
    };
    routes
        .get(&(domain.to_owned(), binding.route.clone()))
        .cloned()
        .unwrap_or_default()
}

fn semantic_route_fragments(
    binding: &SemanticStorageBinding,
    root: &Path,
) -> Result<Vec<PathBuf>, GfError> {
    let (inventory, _) = crate::capture_graph_files(root)?;
    Ok(binding_fragments(
        binding,
        &semantic_fragment_inventory(root, &inventory)?,
    ))
}

fn validate_no_unlisted_semantic_routes(
    graph_root: &Path,
    expected: &BTreeSet<PathBuf>,
) -> Result<(), GfError> {
    for subdir in ["topology/edges", "properties", "edge_properties"] {
        let entries = match std::fs::read_dir(graph_root.join(subdir)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(corrupt("semantic route inventory cannot be read")),
        };
        for entry in entries {
            let entry = entry.map_err(|_| corrupt("semantic route inventory cannot be read"))?;
            let path = entry.path();
            let opaque = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("s-"));
            let file_type = entry
                .file_type()
                .map_err(|_| corrupt("semantic route inventory cannot be inspected"))?;
            if !opaque {
                continue;
            }
            if file_type.is_symlink() || (!file_type.is_file() && !file_type.is_dir()) {
                return Err(corrupt(
                    "opaque semantic route has a noncanonical file type",
                ));
            }
            if file_type.is_file() {
                if path.extension().and_then(|value| value.to_str()) != Some("parquet")
                    || !expected.contains(&path)
                {
                    return Err(corrupt("unlisted opaque semantic route file is present"));
                }
                continue;
            }
            for fragment in std::fs::read_dir(&path)
                .map_err(|_| corrupt("semantic shard route cannot be read"))?
            {
                let fragment =
                    fragment.map_err(|_| corrupt("semantic shard fragment cannot be read"))?;
                let fragment_path = fragment.path();
                let fragment_type = fragment
                    .file_type()
                    .map_err(|_| corrupt("semantic shard fragment cannot be inspected"))?;
                if !fragment_type.is_file()
                    || fragment_path.extension().and_then(|value| value.to_str()) != Some("parquet")
                    || !expected.contains(&fragment_path)
                {
                    return Err(corrupt("unlisted opaque semantic route shard is present"));
                }
            }
        }
    }
    Ok(())
}

fn validate_semantic_route_fragment(
    binding: &SemanticStorageBinding,
    path: &Path,
    graph_root: &Path,
    inventory_paths: Option<&BTreeSet<&str>>,
    composition_fingerprint: &str,
) -> Result<(), GfError> {
    let relative = path
        .strip_prefix(graph_root)
        .map_err(|_| corrupt("semantic route escapes graph inventory"))?
        .to_string_lossy()
        .replace('\\', "/");
    if inventory_paths.is_some_and(|paths| !paths.contains(relative.as_str())) {
        return Err(corrupt(
            "semantic route is absent from authenticated graph inventory",
        ));
    }
    let builder = admitted_semantic_parquet(path)?;
    let schema = builder.schema();
    if schema.fields().len() > MAX_SEMANTIC_PARQUET_COLUMNS {
        return Err(corrupt("semantic route column count exceeds limit"));
    }
    if schema
        .metadata()
        .get(SEMANTIC_ROUTE_METADATA_KEY)
        .map(String::as_str)
        != Some(binding.route.as_str())
        || schema
            .metadata()
            .get(SEMANTIC_COMPOSITION_METADATA_KEY)
            .map(String::as_str)
            != Some(composition_fingerprint)
    {
        return Err(corrupt(&format!(
            "semantic route metadata does not match binding for {relative}"
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
    Ok(())
}

impl SemanticStorageBinding {
    /// Legacy logical route path, or none for numeric entity bindings.
    /// Mapped graph files must be resolved through their admitted route inventory.
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
            let metadata = admitted_semantic_parquet(&path)?
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

fn semantic_route_from_wire(relative: &str) -> Option<&str> {
    let parts = relative.split('/').collect::<Vec<_>>();
    let route = match parts.as_slice() {
        ["topology", "edges", route]
        | ["topology", "edges", route, _]
        | ["properties" | "edge_properties", route]
        | ["properties" | "edge_properties", route, _] => *route,
        _ => return None,
    };
    Some(route.strip_suffix(".parquet").unwrap_or(route)).filter(|route| route.starts_with("s-"))
}

fn corrupt(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn admitted_semantic_parquet(
    path: &Path,
) -> Result<parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<File>, GfError> {
    crate::catalog::admitted_parquet(path)
        .map_err(|error| corrupt(&format!("semantic Parquet admission failed: {error}")))
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

fn checked_semantic_storage_id(id: u32) -> Result<(), GfError> {
    graphforge_value::TaggedTypeId::ontology(graphforge_core::TypeId(id))
        .map(|_| ())
        .map_err(|error| corrupt(&format!("invalid semantic storage id: {error}")))
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

    #[test]
    fn semantic_parquet_admission_rejects_directory_and_bad_leading_magic() {
        let root = tempfile::TempDir::new().unwrap();
        assert!(admitted_semantic_parquet(root.path()).is_err());
        let bad = root.path().join("bad.parquet");
        std::fs::write(&bad, b"NOPE\0\0\0\0PAR1").unwrap();
        assert!(
            admitted_semantic_parquet(&bad)
                .unwrap_err()
                .to_string()
                .contains("leading magic")
        );
        let bad_footer = root.path().join("bad-footer.parquet");
        std::fs::write(&bad_footer, b"PAR1\0\0\0\0NOPE").unwrap();
        assert!(admitted_semantic_parquet(&bad_footer).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn semantic_parquet_admission_rejects_symlink_and_fifo_without_blocking() {
        use std::os::unix::fs::symlink;

        let root = tempfile::TempDir::new().unwrap();
        let target = root.path().join("target.parquet");
        std::fs::write(&target, b"NOPE\0\0\0\0PAR1").unwrap();
        let link = root.path().join("link.parquet");
        symlink(&target, &link).unwrap();
        assert!(admitted_semantic_parquet(&link).is_err());

        let fifo = root.path().join("pipe.parquet");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert!(admitted_semantic_parquet(&fifo).is_err());
    }
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
    fn semantic_topology_preserves_absent_primary_after_label_addition() {
        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let node = graphforge_core::uuid::new_v7();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap();
        writer.create_node_with_labels(node, &[]).unwrap();
        let label =
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(entity.storage_id))
                .unwrap();
        assert_eq!(writer.add_pending_node_labels(node.as_bytes(), &[label]), 1);
        writer.flush().unwrap();
        let paths = crate::catalog::topology_node_files(dir.path()).unwrap();
        let before: Vec<_> = paths
            .iter()
            .map(std::fs::read)
            .collect::<Result<_, _>>()
            .unwrap();
        bindings.validate_physical_routes(dir.path()).unwrap();
        assert_eq!(
            paths
                .iter()
                .map(std::fs::read)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            before
        );
    }

    #[test]
    fn semantic_topology_preserves_known_primary_after_label_removal() {
        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let label =
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(entity.storage_id))
                .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let node = graphforge_core::uuid::new_v7();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap();
        writer.create_node(node, label).unwrap();
        assert_eq!(
            writer.remove_pending_node_labels(node.as_bytes(), &[label]),
            1
        );
        writer.flush().unwrap();
        let paths = crate::catalog::topology_node_files(dir.path()).unwrap();
        let before: Vec<_> = paths
            .iter()
            .map(std::fs::read)
            .collect::<Result<_, _>>()
            .unwrap();
        bindings.validate_physical_routes(dir.path()).unwrap();
        let nodes = crate::read_nodes(dir.path()).unwrap();
        let primary = nodes[0]
            .column_by_name("type_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap();
        assert_eq!(primary.value(0), label.encode());
        assert_eq!(
            paths
                .iter()
                .map(std::fs::read)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            before
        );
    }

    #[test]
    fn semantic_storage_id_codec_checks_canonical_decode_boundaries() {
        let make = |storage_id| {
            let symbol = symbol("a", SymbolKind::Entity, "Person");
            SemanticStorageBinding {
                route_kind: SemanticRouteKind::Entity,
                storage_id,
                route: SemanticStorageBindings::opaque_route(
                    SemanticRouteKind::Entity,
                    &symbol,
                    None,
                ),
                symbol,
                owner: None,
            }
        };
        let valid = SemanticStorageBindings::new(
            "1".repeat(64),
            vec![make(graphforge_value::TYPE_LOCAL_ID_LIMIT - 1)],
        )
        .unwrap();
        let bytes = valid.to_canonical_json().unwrap();
        assert_eq!(
            SemanticStorageBindings::from_canonical_json(&bytes)
                .unwrap()
                .to_canonical_json()
                .unwrap(),
            bytes
        );
        for invalid in [graphforge_value::TYPE_LOCAL_ID_LIMIT, 1 << 31, u32::MAX] {
            assert!(SemanticStorageBindings::new("1".repeat(64), vec![make(invalid)]).is_err());
            let mut wire = serde_json::to_value(&valid).unwrap();
            wire["bindings"][0]["storage_id"] = serde_json::json!(invalid);
            assert!(
                SemanticStorageBindings::from_canonical_json(&serde_json::to_vec(&wire).unwrap())
                    .is_err()
            );
        }
    }

    #[test]
    fn semantic_topology_rejects_invalid_memberships_without_rewrite() {
        use arrow::array::{Array, ListArray, UInt32Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;
        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let label =
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(entity.storage_id))
                .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap();
        writer
            .create_node(graphforge_core::uuid::new_v7(), label)
            .unwrap();
        writer.flush().unwrap();
        let path = crate::catalog::topology_node_files(dir.path())
            .unwrap()
            .remove(0);
        let batch = crate::read_nodes(dir.path()).unwrap().remove(0);
        let column = batch.schema().index_of("type_ids").unwrap();
        let original = batch
            .column(column)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let DataType::List(field) = original.data_type() else {
            panic!("list membership")
        };
        for invalid in [1 << 31, u32::MAX, entity.storage_id + 999] {
            let mut columns = batch.columns().to_vec();
            columns[column] = Arc::new(ListArray::new(
                field.clone(),
                original.offsets().clone(),
                Arc::new(UInt32Array::from(vec![invalid])),
                None,
            ));
            let malformed =
                arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).unwrap();
            let mut output = parquet::arrow::ArrowWriter::try_new(
                File::create(&path).unwrap(),
                malformed.schema(),
                None,
            )
            .unwrap();
            output.write(&malformed).unwrap();
            output.close().unwrap();
            let before = std::fs::read(&path).unwrap();
            assert!(
                bindings.validate_physical_routes(dir.path()).is_err(),
                "must reject membership {invalid}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "rejection must not rewrite topology"
            );
        }
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

    pub(super) fn compiled_with(
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

    pub(super) fn compiled(version: &str) -> CompiledComposition {
        compiled_with(version, true, true)
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
            .create_node(
                left,
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(
                    entity.storage_id,
                ))
                .unwrap(),
            )
            .unwrap();
        writer
            .create_node(
                right,
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(
                    entity.storage_id,
                ))
                .unwrap(),
            )
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
        let authority =
            crate::graph_projection::TransformRoutes::from_inventory(dir.path(), inventory.clone())
                .unwrap();
        omitted.files.retain(|entry| {
            authority.semantic_path(&entry.relative_path).unwrap()
                != format!("topology/edges/{}.parquet", relation.route)
        });
        assert_eq!(omitted.files.len() + 1, inventory.files.len());
        omitted.file_count = omitted.files.len() as u64;
        assert!(
            bindings
                .validate_physical_routes_with_inventory(dir.path(), Some(&omitted))
                .is_err()
        );
        assert!(require_atomic_legacy_migration(dir.path()).is_err());

        let injected = dir.path().join("properties/s-deadbeef.parquet");
        std::fs::write(injected, b"unlisted route must be rejected before decode").unwrap();
        assert!(bindings.validate_physical_routes(dir.path()).is_err());
    }

    #[test]
    fn topology_validation_accepts_mixed_legacy_and_normalized_fragments() {
        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(composition.fingerprint.clone()));
        writer
            .create_node(
                graphforge_core::uuid::new_v7(),
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(
                    entity.storage_id,
                ))
                .unwrap(),
            )
            .unwrap();
        writer.flush().unwrap();

        let current_path = crate::catalog::topology_node_files(dir.path())
            .unwrap()
            .remove(0);
        let current_batch = admitted_semantic_parquet(&current_path)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let legacy_fields = current_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| (index != 3).then_some(field.as_ref().clone()))
            .collect::<Vec<_>>();
        let legacy_columns = current_batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, column)| (index != 3).then_some(std::sync::Arc::clone(column)))
            .collect::<Vec<_>>();
        let legacy = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(legacy_fields)),
            legacy_columns,
        )
        .unwrap();
        let canonical_dir = dir.path().join("topology/nodes");
        std::fs::create_dir_all(&canonical_dir).unwrap();
        let retained_current =
            canonical_dir.join("00000000000000000001-00000000000000000001.parquet");
        if current_path != retained_current {
            std::fs::rename(&current_path, &retained_current).unwrap();
        }
        let legacy_path = dir.path().join("topology/nodes.parquet");
        let mut legacy_writer = parquet::arrow::ArrowWriter::try_new(
            File::create(legacy_path).unwrap(),
            legacy.schema(),
            None,
        )
        .unwrap();
        legacy_writer.write(&legacy).unwrap();
        legacy_writer.close().unwrap();

        bindings.validate_physical_routes(dir.path()).unwrap();
    }

    #[test]
    fn semantic_validation_covers_every_immutable_property_fragment() {
        use std::collections::HashMap;

        let composition = compiled("1");
        let bindings = SemanticStorageBindings::project(&composition, None).unwrap();
        let entity = bindings
            .bindings
            .iter()
            .find(|binding| binding.route_kind == SemanticRouteKind::Entity)
            .unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(composition.fingerprint.clone()));
        let node = graphforge_core::uuid::new_v7();
        writer
            .create_node(
                node,
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(
                    entity.storage_id,
                ))
                .unwrap(),
            )
            .unwrap();
        writer
            .set_properties(
                &node,
                Some(&entity.route),
                HashMap::from([("name".into(), graphforge_ir::IrLiteral::Str("Ada".into()))]),
            )
            .unwrap();
        writer.flush().unwrap();
        let mut second =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 2)
                .unwrap()
                .with_semantic_composition_fingerprint(Some(composition.fingerprint.clone()));
        second
            .set_properties(
                &node,
                Some(&entity.route),
                HashMap::from([("name".into(), graphforge_ir::IrLiteral::Str("Grace".into()))]),
            )
            .unwrap();
        second.flush().unwrap();

        let inventory = crate::capture_graph_files(dir.path()).unwrap().0;
        let fragments = semantic_fragment_inventory(dir.path(), &inventory)
            .unwrap()
            .remove(&("properties".to_owned(), entity.route.clone()))
            .unwrap();
        assert_eq!(fragments.len(), 2);
        let authority =
            crate::graph_projection::TransformRoutes::from_inventory(dir.path(), inventory.clone())
                .unwrap();
        let rows = authority
            .property_batches(dir.path(), &entity.route, false)
            .unwrap();
        let names = rows[0]
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::StringArray>())
            .unwrap();
        assert_eq!(names.value(0), "Grace");

        bindings.validate_physical_routes(dir.path()).unwrap();
        let (inventory, _) = crate::capture_graph_files(dir.path()).unwrap();
        bindings
            .validate_physical_routes_with_inventory(dir.path(), Some(&inventory))
            .unwrap();
        for fragment in fragments {
            let relative = fragment
                .strip_prefix(dir.path())
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            let mut omitted = inventory.clone();
            omitted
                .files
                .retain(|entry| entry.relative_path != relative);
            omitted.file_count = omitted.files.len() as u64;
            assert!(
                bindings
                    .validate_physical_routes_with_inventory(dir.path(), Some(&omitted))
                    .is_err(),
                "omitting authenticated property fragment {relative} must fail closed"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn semantic_route_inventory_rejects_opaque_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path().join("properties");
        std::fs::create_dir_all(&root).unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, b"not parquet").unwrap();
        symlink(&target, root.join("s-deadbeef.parquet")).unwrap();
        assert!(validate_no_unlisted_semantic_routes(dir.path(), &BTreeSet::new()).is_err());
    }
}
