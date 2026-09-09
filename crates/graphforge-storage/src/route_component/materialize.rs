//! Versioned translation at the boundary into an empty private workspace.

use std::io::Write;
use std::path::Path;

use graphforge_core::GfError;
use graphforge_filesystem::StableDirectory;

use super::{RouteTable, TABLE_FILE, authenticate_manifest_routes, encode_relative_route, invalid};

pub(crate) struct MaterializationRoutes {
    pub(crate) destinations: Vec<String>,
    pub(crate) legacy: bool,
    table: Option<Vec<u8>>,
}

impl MaterializationRoutes {
    pub(crate) fn prepare(
        inventory: &crate::GraphFilesInventory,
        read: impl FnOnce(&crate::GraphFileEntry) -> Result<Vec<u8>, GfError>,
    ) -> Result<Self, GfError> {
        let admitted =
            authenticate_manifest_routes(inventory.format_version, &inventory.files, read)?;
        let legacy = admitted.is_none();
        if legacy && inventory.files.len() >= 100_000 {
            return Err(super::limit(
                "materialized route table exceeds inventory entry budget",
            ));
        }
        let mut table = RouteTable::default();
        let mut destinations = Vec::with_capacity(inventory.files.len());
        let mut seen = std::collections::BTreeSet::new();
        for entry in &inventory.files {
            let destination = if legacy {
                let logical =
                    crate::graph_files::legacy_inventory_logical_text(&entry.relative_path)?;
                encode_relative_route(&logical, &mut table, 64 * 1024 * 1024, 100_000)?
            } else {
                entry.relative_path.clone()
            };
            crate::graph_files::wire_relative_path(&destination)?;
            if !seen.insert(destination.clone()) {
                return Err(invalid("materialized route destinations collide"));
            }
            destinations.push(destination);
        }
        Ok(Self {
            destinations,
            legacy,
            table: legacy.then(|| table.encode(64 * 1024 * 1024)).transpose()?,
        })
    }

    /// Install only into the caller's empty, unpublished materialization. The
    /// source inventory remains immutable and retains its original version.
    pub(crate) fn install_table(
        &self,
        target: &Path,
        evidence: &mut crate::GraphFilesOpenEvidence,
    ) -> Result<(), GfError> {
        let Some(bytes) = &self.table else {
            return Ok(());
        };
        let directory = StableDirectory::open(target)
            .map_err(|_| invalid("materialized route directory cannot be retained"))?;
        let name = std::ffi::OsString::from(format!(
            "semantic-routes.json.{}.tmp",
            uuid::Uuid::new_v4().simple()
        ));
        let mut file = directory
            .create_replaceable_child_file(&name)
            .map_err(|_| invalid("materialized route table cannot be staged"))?;
        let identity = graphforge_filesystem::file_identity(&file)
            .map_err(|_| invalid("materialized route table identity unavailable"))?;
        let result = (|| {
            let mut remaining = bytes.as_slice();
            while !remaining.is_empty() {
                let written = file
                    .write(&remaining[..remaining.len().min(64 * 1024)])
                    .map_err(|_| invalid("materialized route table write failed"))?;
                if written == 0 {
                    return Err(invalid("materialized route table write stopped"));
                }
                evidence.application_write_bytes = evidence
                    .application_write_bytes
                    .checked_add(written as u64)
                    .ok_or_else(|| invalid("materialized route write bytes overflow"))?;
                evidence.application_write_calls = evidence
                    .application_write_calls
                    .checked_add(1)
                    .ok_or_else(|| invalid("materialized route write calls overflow"))?;
                remaining = &remaining[written..];
            }
            file.sync_all()
                .map_err(|_| invalid("materialized route table sync failed"))?;
            evidence.file_fsync_calls = evidence
                .file_fsync_calls
                .checked_add(1)
                .ok_or_else(|| invalid("materialized file sync count overflow"))?;
            evidence.fsync_calls = evidence
                .fsync_calls
                .checked_add(1)
                .ok_or_else(|| invalid("materialized sync count overflow"))?;
            directory
                .install_child(&name, identity, std::ffi::OsStr::new(TABLE_FILE))
                .map_err(|_| invalid("materialized route table install failed"))?;
            directory
                .sync()
                .map_err(|_| invalid("materialized route directory sync failed"))?;
            evidence.directory_fsync_calls = evidence
                .directory_fsync_calls
                .checked_add(1)
                .ok_or_else(|| invalid("materialized directory sync count overflow"))?;
            evidence.fsync_calls = evidence
                .fsync_calls
                .checked_add(1)
                .ok_or_else(|| invalid("materialized sync count overflow"))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = directory.unlink_child_if_identity(&name, identity);
        }
        result
    }
}
