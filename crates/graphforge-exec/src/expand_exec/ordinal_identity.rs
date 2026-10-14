//! Generation-pinned destination identity shared by every traversal.

use graphforge_core::GfError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Generation-pinned destination-identity resolver shared by every query and
/// hop of one facade. Replacement is atomic with facade generation adoption;
/// execution never rediscovers mutable identity files by path.
#[derive(Debug, Default)]
pub struct V4OrdinalIdentityResolver {
    handle: RwLock<
        Option<Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>>,
    >,
}

pub(crate) struct V4OrdinalIdentityPin {
    pub(crate) session: Option<Arc<V4OrdinalIdentitySession>>,
    pub(crate) required: bool,
}

impl V4OrdinalIdentityResolver {
    /// Construct a resolver for an optional admitted generation facet.
    #[must_use]
    pub fn new(
        handle: Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>,
    ) -> Self {
        Self {
            handle: RwLock::new(handle.map(|handle| Arc::new(Mutex::new(handle)))),
        }
    }

    /// Replace the exact generation served by subsequent sessions.
    pub fn replace(
        &self,
        handle: Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>,
    ) {
        *self.handle.write().expect("ordinal identity lock poisoned") =
            handle.map(|handle| Arc::new(Mutex::new(handle)));
    }

    /// The exact generation this resolver serves, revalidated (by stat, no
    /// artifact bytes) for one caller outside query execution, such as search
    /// membership projection. `None` when the facade has no ordinal authority.
    ///
    /// # Errors
    /// Returns the refusal when the retained artifacts no longer match the
    /// generation the handle authenticated.
    pub fn revalidated_handle(
        &self,
    ) -> Result<
        Option<Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>>,
        GfError,
    > {
        let handle = self
            .handle
            .read()
            .expect("ordinal identity lock poisoned")
            .clone();
        if let Some(handle) = &handle {
            handle
                .lock()
                .expect("ordinal identity handle poisoned")
                .revalidate_for_session()
                .map_err(GfError::from)?;
        }
        Ok(handle)
    }

    pub(crate) fn pin(&self, query_budget_bytes: u64) -> Result<V4OrdinalIdentityPin, GfError> {
        let handle = self
            .handle
            .read()
            .expect("ordinal identity lock poisoned")
            .clone();
        let Some(handle) = handle else {
            return Ok(V4OrdinalIdentityPin {
                session: None,
                required: false,
            });
        };
        let revalidation = {
            let authority = handle.lock().expect("ordinal identity handle poisoned");
            let supplied = u64::try_from(authority.cache_budget_bytes()).unwrap_or(u64::MAX);
            let allowed =
                graphforge_storage::read_cache_budget::ReadCacheBudget::from_query_budget(
                    query_budget_bytes,
                )
                .identity_bytes;
            if supplied > allowed {
                return Err(GfError::Api {
                    code: graphforge_core::ApiErrorCode::ResourceLimit,
                    message: format!(
                        "supplied ordinal identity has a {supplied} byte cache ceiling against the session's {allowed} byte read-cache share"
                    ),
                });
            }
            authority.revalidate_for_session().map_err(GfError::from)?
        };
        Ok(V4OrdinalIdentityPin {
            session: Some(Arc::new(V4OrdinalIdentitySession {
                handle,
                revalidation,
                attribution_available: AtomicBool::new(true),
            })),
            required: true,
        })
    }
}

/// One exact, already-authenticated ordinal authority pinned for the lifetime
/// of an execution session.
#[derive(Debug)]
pub(crate) struct V4OrdinalIdentitySession {
    handle: Arc<Mutex<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>>,
    revalidation: graphforge_storage::V4OrdinalRevalidationMetrics,
    attribution_available: AtomicBool,
}

impl V4OrdinalIdentitySession {
    pub(crate) fn max_requested_ids(&self) -> usize {
        self.handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .max_requested_ids()
    }

    /// Whether node-ordinal order is UUID order. The first call reads and
    /// authenticates every ordinal block once; a corrupted block is an error,
    /// never an "unordered" answer.
    pub(crate) fn uuid_order_matches_ordinals(&self) -> Result<bool, GfError> {
        self.handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .uuid_order_matches_ordinals()
            .map_err(GfError::from)
    }

    pub(crate) fn lookup_node_uuids(
        &self,
        requested: &[u64],
    ) -> Result<graphforge_storage::V4OrdinalLookup, GfError> {
        let mut lookup = self
            .handle
            .lock()
            .expect("ordinal identity handle poisoned")
            .lookup_node_uuids_pinned(requested)
            .map_err(GfError::from)?;
        if self.attribution_available.swap(false, Ordering::AcqRel) {
            lookup.metrics.revalidation_calls = self.revalidation.calls;
            lookup.metrics.revalidation_bytes = self.revalidation.bytes_read;
        }
        Ok(lookup)
    }
}
