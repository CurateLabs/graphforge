//! Explicit rebuild of the persisted property indexes (#1983, #1984).
//!
//! A bulk construction publishes the property-equality index and the edge
//! property index inside its own generation. A project written some other way
//! (published before the indexes existed, or changed by a mutation since)
//! answers `{id: $x}` through the scan and edge properties through the property
//! overlay until this runs: a read never builds or repairs an index.

use std::path::{Path, PathBuf};

use super::{CancellationToken, GfError, GraphForge};

pub use graphforge_storage::edge_property_index::EdgePropertyIndexBuild;
pub use graphforge_storage::property_equality_index::PropertyEqualityIndexBuild;

/// What one rebuild of the persisted property indexes wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PropertyIndexBuild {
    /// The node property-equality index.
    pub equality: PropertyEqualityIndexBuild,
    /// The edge property index.
    pub edge_properties: EdgePropertyIndexBuild,
}

/// One index directory being replaced: the prior tree is kept beside the
/// project until the publication that makes the replacement authoritative.
struct DirectorySwap {
    live: PathBuf,
    backup: PathBuf,
    had_prior: bool,
    installed: bool,
}

impl DirectorySwap {
    /// Move the prior tree, if any, out of the way.
    fn begin(project: &Path, live: PathBuf) -> Result<Self, GfError> {
        let backup = project.with_file_name(format!(
            ".graphforge-property-index-backup.{}",
            uuid::Uuid::new_v4().simple()
        ));
        let had_prior = live.exists();
        if had_prior {
            std::fs::rename(&live, &backup).map_err(|error| {
                GfError::Storage(format!("cannot preserve prior property index: {error}"))
            })?;
        }
        Ok(Self {
            live,
            backup,
            had_prior,
            installed: false,
        })
    }

    /// Move the staged tree into place.
    fn install(&mut self, staged: &Path) -> Result<(), GfError> {
        if let Some(parent) = self.live.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                GfError::Storage(format!(
                    "cannot prepare property index destination: {error}"
                ))
            })?;
        }
        std::fs::rename(staged, &self.live)
            .map_err(|error| GfError::Storage(format!("cannot install property index: {error}")))?;
        self.installed = true;
        Ok(())
    }

    /// Put the prior tree back.
    fn restore(&self) -> Result<(), GfError> {
        if self.installed {
            std::fs::remove_dir_all(&self.live).map_err(|error| {
                GfError::Storage(format!("cannot remove failed property index: {error}"))
            })?;
        }
        if self.had_prior {
            std::fs::rename(&self.backup, &self.live).map_err(|error| {
                GfError::Storage(format!("cannot restore prior property index: {error}"))
            })?;
        }
        Ok(())
    }

    /// Publication is authoritative; a failed cleanup must not turn a committed
    /// rebuild into a false failure.
    fn commit(&self) {
        if self.had_prior {
            let _ = std::fs::remove_dir_all(&self.backup);
        }
    }
}

impl GraphForge {
    /// Rebuild the persisted property indexes from the current generation, and
    /// publish them as a new generation: the equality index of every node
    /// property route and the edge property index of every edge property route.
    ///
    /// # Errors
    /// Returns the route read, cancellation, write or publication failure. A
    /// failed publication restores the prior indexes.
    pub fn rebuild_property_indexes(
        &self,
        cancellation: Option<CancellationToken>,
    ) -> Result<PropertyIndexBuild, GfError> {
        let _phase = graphforge_storage::lifecycle_io::PhaseScope::enter(
            graphforge_storage::StorageIoPhase::EncodeWritePostwriteAuthentication,
        );
        let visibility = self.graph_visibility.lock()?;
        let token = cancellation.unwrap_or_default();
        token.checkpoint()?;
        let workspace = self.workspace_for_session();
        let workspace_parent = workspace.path().parent().ok_or_else(|| {
            GfError::Storage("graph workspace has no same-filesystem parent".into())
        })?;
        let staged = tempfile::Builder::new()
            .prefix(".graphforge-property-index-stage.")
            .tempdir_in(workspace_parent)
            .map_err(|error| GfError::Storage(format!("cannot stage property index: {error}")))?;
        let inventory = self.property_inventory_for_session();
        let equality =
            graphforge_storage::property_equality_index::write_property_equality_indexes(
                staged.path(),
                &inventory,
                &mut || token.checkpoint(),
            )?;
        let edge_properties = graphforge_storage::edge_property_index::write_edge_property_indexes(
            staged.path(),
            &inventory,
            &mut || token.checkpoint(),
        )?;
        token.checkpoint()?;

        let project = self.dir();
        let plan = [
            (
                graphforge_storage::property_equality_index::index_dir(&project),
                graphforge_storage::property_equality_index::index_dir(staged.path()),
                equality.files > 0,
            ),
            (
                graphforge_storage::edge_property_index::index_dir(&project),
                graphforge_storage::edge_property_index::index_dir(staged.path()),
                edge_properties.files > 0,
            ),
        ];
        let mut swaps: Vec<DirectorySwap> = Vec::new();
        let mut outcome = Ok(());
        for (live, staged_dir, install) in plan {
            match DirectorySwap::begin(&project, live) {
                Ok(swap) => swaps.push(swap),
                Err(error) => {
                    outcome = Err(error);
                    break;
                }
            }
            if install {
                let swap = swaps.last_mut().expect("swap just pushed");
                if let Err(error) = swap.install(&staged_dir) {
                    outcome = Err(error);
                    break;
                }
            }
        }
        if outcome.is_ok() {
            outcome = self.publish_workspace_update();
        }
        if let Err(error) = outcome {
            for swap in swaps.iter().rev() {
                swap.restore()?;
            }
            return Err(error);
        }
        for swap in &swaps {
            swap.commit();
        }
        drop(visibility);
        Ok(PropertyIndexBuild {
            equality,
            edge_properties,
        })
    }
}
