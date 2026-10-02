//! Explicit topology membership for a generation and its session-owned writes.
//!
//! Admission checks bytes; it does not grant membership. Readers receive this
//! file list and cannot discover additional payloads by listing a directory.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use graphforge_core::GfError;

/// Canonical physical topology payloads selected by one read authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopologyFiles {
    pub(crate) nodes: Vec<(PathBuf, String)>,
    pub(crate) edges: Vec<(String, PathBuf, String)>,
}

impl TopologyFiles {
    /// Resolve only the files declared by an authenticated full inventory.
    pub fn from_inventory(
        inventory: &crate::AuthenticatedPropertyInventory,
    ) -> Result<Self, GfError> {
        Ok(Self {
            nodes: inventory.node_fragments().ok_or_else(missing_authority)?,
            edges: inventory.edge_fragments(None),
        })
    }

    /// Explicit discovery boundary for a manifest-less standalone/legacy tree.
    /// Compact sessions construct their authority from the manifest instead.
    pub fn discover_legacy(root: &Path) -> Result<Self, GfError> {
        let nodes = crate::mutator::node_parquet_files(root)?
            .into_iter()
            .map(|path| Ok((path.clone(), relative(root, &path)?)))
            .collect::<Result<_, GfError>>()?;
        let table = if root.exists() {
            let directory = graphforge_filesystem::StableDirectory::open(root)
                .map_err(|error| GfError::Storage(error.to_string()))?;
            crate::route_component::owned::read_owned_layout_table(&directory)?
        } else {
            None
        };
        let edges = crate::mutator::edge_parquet_files(root, None)?
            .into_iter()
            .map(|(route, path)| {
                let name = relative(root, &path)?;
                let route = if let Some(table) = &table {
                    let semantic = table.semantic_relative_path(&name)?;
                    crate::route_component::route_position(&semantic)?
                        .ok_or_else(|| GfError::Storage("owned edge lacks route".into()))?
                        .to_owned()
                } else {
                    route
                };
                Ok((route, path, name))
            })
            .collect::<Result<_, GfError>>()?;
        Ok(Self { nodes, edges })
    }

    /// Node files, in canonical order, with manifest-relative names.
    #[must_use]
    pub fn node_fragments(&self) -> &[(PathBuf, String)] {
        &self.nodes
    }

    /// Edge files, retaining the semantic route independently of physical names.
    #[must_use]
    pub fn edge_fragments(&self) -> &[(String, PathBuf, String)] {
        &self.edges
    }

    pub(crate) fn at_root(mut self, root: &Path) -> Self {
        for (path, relative) in &mut self.nodes {
            *path = root.join(relative);
        }
        for (_, path, relative) in &mut self.edges {
            *path = root.join(relative);
        }
        self
    }

    pub(crate) fn paths(&self) -> impl Iterator<Item = &Path> {
        self.nodes
            .iter()
            .map(|(path, _)| path.as_path())
            .chain(self.edges.iter().map(|(_, path, _)| path.as_path()))
    }
}

/// Session-owned membership. Only completed staged writes extend this list.
#[derive(Debug)]
pub struct TopologyFileAuthority {
    root: PathBuf,
    files: RwLock<TopologyFiles>,
}

impl TopologyFileAuthority {
    /// Bind a private workspace to its declared generation topology.
    pub fn from_inventory(
        root: &Path,
        inventory: &crate::AuthenticatedPropertyInventory,
    ) -> Result<Arc<Self>, GfError> {
        let mut files = TopologyFiles::from_inventory(inventory)?;
        let directory = graphforge_filesystem::StableDirectory::open(root)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if crate::route_component::owned::read_owned_layout_table(&directory)?.is_some() {
            // Legacy manifests name semantic routes, while their private
            // materialization uses mapped components. Preserve the declared
            // semantic route and translate only the physical spelling.
            for (route, _, name) in &mut files.edges {
                if crate::route_component::route_position(name)?.is_none() {
                    continue;
                }
                let mapped = crate::route_component::component(route);
                let mut parts: Vec<_> = name.split('/').map(str::to_owned).collect();
                parts[2] = if parts.len() == 3 {
                    format!("{mapped}.parquet")
                } else {
                    mapped
                };
                *name = parts.join("/");
            }
        }
        Ok(Arc::new(Self {
            root: root.to_path_buf(),
            files: RwLock::new(files.at_root(root)),
        }))
    }

    /// Establish the authority of a manifest-less tree at the legacy boundary.
    pub fn discover_legacy(root: &Path) -> Result<Arc<Self>, GfError> {
        Ok(Arc::new(Self {
            root: root.to_path_buf(),
            files: RwLock::new(TopologyFiles::discover_legacy(root)?),
        }))
    }

    pub(crate) fn prepare_installed(
        &self,
        staged: &crate::RewriteBatch,
    ) -> Result<TopologyFiles, GfError> {
        Ok(enumerate_topology_files(self, Some(staged))?.at_root(&self.root))
    }

    pub(crate) fn install(&self, candidate: TopologyFiles) {
        *self
            .files
            .write()
            .expect("topology authority lock poisoned") = candidate;
    }

    /// Restore a membership snapshot retained by this session's rollback owner.
    pub fn restore(&self, files: TopologyFiles) {
        self.install(files);
    }

    /// Reconcile a materialized workspace with the selected durable authority.
    pub fn replace_from_inventory(
        &self,
        inventory: &crate::AuthenticatedPropertyInventory,
    ) -> Result<(), GfError> {
        let selected = Self::from_inventory(&self.root, inventory)?;
        self.install(enumerate_topology_files(&selected, None)?);
        Ok(())
    }

    /// Reset after the owning facade deliberately clears its private tree.
    pub fn clear(&self) {
        self.install(TopologyFiles::default());
    }
}

/// The one enumeration operation: declared/owned files plus this batch's own
/// retained temporary replacements. No directory discovery occurs here.
pub fn enumerate_topology_files(
    authority: &TopologyFileAuthority,
    staged: Option<&crate::RewriteBatch>,
) -> Result<TopologyFiles, GfError> {
    let mut files = authority
        .files
        .read()
        .expect("topology authority lock poisoned")
        .clone();
    if let Some(staged) = staged {
        let retired = staged.retired_relative_paths();
        files.nodes.retain(|(_, name)| !retired.contains(name));
        files.edges.retain(|(_, _, name)| !retired.contains(name));
        for destination in staged.staged_paths() {
            let name = relative(&authority.root, destination)?;
            let physical = staged
                .staged_temp(destination)
                .ok_or_else(|| GfError::Storage("owned topology temporary disappeared".into()))?
                .to_path_buf();
            if is_node(&name) {
                if name != "topology/nodes.parquet" {
                    crate::mutator::canonical_topology_shard_range(destination, "node")?;
                }
                if let Some((path, _)) = files.nodes.iter_mut().find(|(_, prior)| prior == &name) {
                    *path = physical;
                } else {
                    files.nodes.push((physical, name));
                }
            } else if name.starts_with("topology/edges/") && name.ends_with(".parquet") {
                let semantic = staged.semantic_relative_path(&authority.root, &name)?;
                let route = crate::route_component::route_position(&semantic)?
                    .ok_or_else(|| GfError::Storage("owned edge lacks route".into()))?;
                if let Some((prior_route, path, _)) =
                    files.edges.iter_mut().find(|(_, _, prior)| prior == &name)
                {
                    *path = physical;
                    route.clone_into(prior_route);
                } else {
                    files.edges.push((route.to_owned(), physical, name));
                }
            }
        }
        files.nodes.sort_by(|a, b| a.1.cmp(&b.1));
        files.edges.sort_by(|a, b| a.2.cmp(&b.2));
    }
    Ok(files)
}

pub(crate) fn is_node(relative: &str) -> bool {
    relative == "topology/nodes.parquet"
        || relative.starts_with("topology/nodes/") && relative.ends_with(".parquet")
}

pub(crate) fn is_topology(relative: &Path) -> bool {
    relative == Path::new("topology/nodes.parquet")
        || relative.starts_with("topology/nodes")
        || relative.starts_with("topology/edges")
}

fn relative(root: &Path, path: &Path) -> Result<String, GfError> {
    path.strip_prefix(root)
        .ok()
        .and_then(Path::to_str)
        .map(|name| name.replace('\\', "/"))
        .ok_or_else(|| GfError::Storage("topology file escaped its workspace".into()))
}

pub(crate) fn missing_authority() -> GfError {
    GfError::Storage(
        "GF_TOPOLOGY_AUTHORITY_MISSING: compact graph requires declared topology files".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_core::uuid::Uuid;
    use graphforge_value::EntityTypeId;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn authenticated_roll_forward_installs_committed_topology_membership() {
        let root = tempfile::tempdir().unwrap();
        let authority = TopologyFileAuthority::discover_legacy(root.path()).unwrap();
        let mut writer = crate::GraphWriter::open_with_topology(
            root.path(),
            graphforge_core::OntologyMode::Strict,
            Arc::clone(&authority),
        )
        .unwrap();
        writer
            .create_node(Uuid::from_u128(1), EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(
            enumerate_topology_files(&authority, None)
                .unwrap()
                .node_fragments()
                .len(),
            1
        );

        writer
            .create_node(Uuid::from_u128(2), EntityTypeId::decode(1).unwrap())
            .unwrap();
        crate::durable_rewrite::inject_error_after_durable_intent();
        // The UUID participant authenticates the durable intent and rolls it
        // forward. Successful reconciliation must publish this exact membership.
        writer.flush().unwrap();
        assert_eq!(crate::read_topology_generation(root.path()).unwrap(), 2);
        let selected = enumerate_topology_files(&authority, None).unwrap();
        assert_eq!(selected.node_fragments().len(), 2);
        assert_eq!(
            crate::read_nodes_from_files(&selected)
                .unwrap()
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn staged_node_destinations_participate_in_label_and_delete_reads() {
        let root = tempfile::tempdir().unwrap();
        let authority = TopologyFileAuthority::discover_legacy(root.path()).unwrap();
        let mut writer = crate::GraphWriter::open_with_topology(
            root.path(),
            graphforge_core::OntologyMode::Exploratory,
            Arc::clone(&authority),
        )
        .unwrap();
        let node = Uuid::from_u128(1);
        writer
            .create_node(node, EntityTypeId::decode(1).unwrap())
            .unwrap();
        let mut staged = crate::RewriteBatch::new();
        writer.flush_into(&mut staged).unwrap();
        assert!(
            enumerate_topology_files(&authority, None)
                .unwrap()
                .node_fragments()
                .is_empty()
        );
        assert_eq!(
            enumerate_topology_files(&authority, Some(&staged))
                .unwrap()
                .node_fragments()
                .len(),
            1
        );
        let labels = HashMap::from([(
            node.into_bytes(),
            HashSet::from([EntityTypeId::decode(2).unwrap()]),
        )]);
        assert_eq!(
            crate::stage_add_node_labels(&mut staged, root.path(), &labels).unwrap(),
            1
        );
        let files = enumerate_topology_files(&authority, Some(&staged)).unwrap();
        let batches = crate::read_nodes_from_files(&files).unwrap();
        let values = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .value(0);
        let values = values
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[1, 2]);
        assert_eq!(
            crate::stage_delete_nodes(
                &mut staged,
                root.path(),
                &HashSet::from([node.into_bytes()])
            )
            .unwrap(),
            1
        );
        let files = enumerate_topology_files(&authority, Some(&staged)).unwrap();
        assert_eq!(
            crate::read_nodes_from_files(&files)
                .unwrap()
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
        // Missing selected staged payloads are refused by both mutation readers.
        std::fs::remove_file(&files.node_fragments()[0].0).unwrap();
        assert!(crate::stage_add_node_labels(&mut staged, root.path(), &labels).is_err());
        assert!(
            crate::stage_delete_nodes(
                &mut staged,
                root.path(),
                &HashSet::from([node.into_bytes()])
            )
            .is_err()
        );
    }

    #[test]
    fn staged_edge_destinations_participate_in_delete_reads() {
        let root = tempfile::tempdir().unwrap();
        let authority = TopologyFileAuthority::discover_legacy(root.path()).unwrap();
        let mut writer = crate::GraphWriter::open_with_topology(
            root.path(),
            graphforge_core::OntologyMode::Strict,
            Arc::clone(&authority),
        )
        .unwrap();
        let left = Uuid::from_u128(1);
        let right = Uuid::from_u128(2);
        let edge = Uuid::from_u128(3);
        writer
            .create_node(left, EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer
            .create_node(right, EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer.create_edge(edge, "r-old", &left, &right).unwrap();
        let mut staged = crate::RewriteBatch::new();
        writer.flush_into(&mut staged).unwrap();
        assert!(
            enumerate_topology_files(&authority, None)
                .unwrap()
                .edges
                .is_empty()
        );
        let selected = enumerate_topology_files(&authority, Some(&staged)).unwrap();
        assert_eq!(selected.edges.len(), 1);
        assert_eq!(selected.edges[0].0, "r-old");
        let targets = HashSet::from([edge.into_bytes()]);
        assert_eq!(
            crate::stage_delete_edges(&mut staged, root.path(), &targets).unwrap(),
            1
        );
        let files = enumerate_topology_files(&authority, Some(&staged)).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&files.edges[0].1).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
            0
        );
        std::fs::remove_file(&files.edges[0].1).unwrap();
        assert!(crate::stage_delete_edges(&mut staged, root.path(), &targets).is_err());
    }
}
