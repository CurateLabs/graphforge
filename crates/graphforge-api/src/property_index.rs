//! Explicit rebuild of the persisted property-equality index (#1983).
//!
//! A bulk construction publishes the index inside its own generation. A project
//! written some other way (published before the index existed, or changed by a
//! mutation since) answers `{id: $x}` through the scan until this runs: a read
//! never builds or repairs an index.

use super::{CancellationToken, GfError, GraphForge};

pub use graphforge_storage::property_equality_index::PropertyEqualityIndexBuild;

impl GraphForge {
    /// Rebuild the persisted equality index of every node property route from
    /// the current generation, and publish it as a new generation.
    ///
    /// # Errors
    /// Returns the route read, cancellation, write or publication failure. A
    /// failed publication restores the prior index.
    pub fn rebuild_property_indexes(
        &self,
        cancellation: Option<CancellationToken>,
    ) -> Result<PropertyEqualityIndexBuild, GfError> {
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
        let report = graphforge_storage::property_equality_index::write_property_equality_indexes(
            staged.path(),
            &self.property_inventory_for_session(),
            &mut || token.checkpoint(),
        )?;
        token.checkpoint()?;

        let live = graphforge_storage::property_equality_index::index_dir(&self.dir());
        let staged_index = graphforge_storage::property_equality_index::index_dir(staged.path());
        let backup = self.dir().with_file_name(format!(
            ".graphforge-property-index-backup.{}",
            uuid::Uuid::new_v4().simple()
        ));
        let had_prior = live.exists();
        if had_prior {
            std::fs::rename(&live, &backup).map_err(|error| {
                GfError::Storage(format!("cannot preserve prior property index: {error}"))
            })?;
        }
        let restore = |installed: bool| -> Result<(), GfError> {
            if installed {
                std::fs::remove_dir_all(&live).map_err(|error| {
                    GfError::Storage(format!("cannot remove failed property index: {error}"))
                })?;
            }
            if had_prior {
                std::fs::rename(&backup, &live).map_err(|error| {
                    GfError::Storage(format!("cannot restore prior property index: {error}"))
                })?;
            }
            Ok(())
        };
        let installed = report.files > 0;
        if installed {
            if let Some(parent) = live.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    GfError::Storage(format!(
                        "cannot prepare property index destination: {error}"
                    ))
                })?;
            }
            if let Err(error) = std::fs::rename(&staged_index, &live) {
                restore(false)?;
                return Err(GfError::Storage(format!(
                    "cannot install property index: {error}"
                )));
            }
        }
        if let Err(error) = self.publish_workspace_update() {
            restore(installed)?;
            return Err(error);
        }
        if had_prior {
            // Publication is authoritative; a failed cleanup must not turn a
            // committed rebuild into a false failure.
            let _ = std::fs::remove_dir_all(&backup);
        }
        drop(visibility);
        Ok(report)
    }
}
