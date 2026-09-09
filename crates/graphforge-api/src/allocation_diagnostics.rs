//! First-party, explicit storage qualification context. This is not a public
//! receipt format and does not change ordinary facade options or requests.
use crate::{GfError, GraphForge, PortableV2ImportRequest, PortableV2ImportResult};
use std::path::Path;
use std::sync::atomic::AtomicBool;

#[doc(hidden)]
pub struct StorageAllocationDiagnostics {
    allocation: graphforge_storage::StorageAllocationOperation,
}
impl StorageAllocationDiagnostics {
    /// Admit a bounded private owner continuation before opening a project.
    ///
    /// # Errors
    /// Rejects missing, malformed, inconsistent, or oversized input.
    pub fn read(input: impl std::io::Read) -> Result<Self, GfError> {
        Ok(Self {
            allocation: graphforge_storage::StorageAllocationOperation::read_private(input)?,
        })
    }
    /// Open through the ordinary facade with explicit allocation diagnostics.
    ///
    /// # Errors
    /// Returns ordinary open and diagnostic ownership errors.
    pub fn open(&self, path: &Path) -> Result<GraphForge, GfError> {
        GraphForge::open_with_allocation_diagnostics(path, self.allocation.clone())
    }
    /// Import through the ordinary static facade with explicit diagnostics.
    ///
    /// # Errors
    /// Returns ordinary portable import and diagnostic ownership errors.
    pub fn import(
        &self,
        path: &Path,
        request: &PortableV2ImportRequest,
        cancelled: Option<&AtomicBool>,
    ) -> Result<PortableV2ImportResult, graphforge_core::portable::PortableV2Error> {
        let path = graphforge_storage::StorageAllocationOperation::resolve_project_path(path)
            .map_err(|_| {
                graphforge_core::portable::PortableV2Error::new(
                    graphforge_core::portable::PortableV2ErrorCode::InvalidPath,
                    "invalid diagnostic import target path",
                )
            })?;
        GraphForge::import_portable_v2_with_allocation(
            &path,
            request,
            cancelled,
            Some(&self.allocation),
        )
    }
    /// Return identity-free current and peak allocation for the private consumer.
    ///
    /// # Errors
    /// Refuses a poisoned accounting context.
    pub fn totals(&self) -> Result<(u64, u64), GfError> {
        self.allocation.totals()
    }
}
