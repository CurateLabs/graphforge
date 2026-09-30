//! Request identities shared by every attempt of one immutable publication.

use std::cell::OnceCell;

use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, ProjectErrorCode};
use uuid::Uuid;

use super::CompositeTransactionRequest;
use crate::composite_receipt::{build_composite_receipt, composite_generation_uuid};

/// The borrow prevents request mutation while its canonical identities are reused.
/// Parent-dependent authorization is deliberately outside this cache.
pub(crate) struct PreparedCompositeOperation<'request> {
    request: &'request CompositeTransactionRequest,
    fingerprint: [u8; 32],
    generation_uuid: Uuid,
    receipt: OnceCell<RecordBatch>,
}

impl<'request> PreparedCompositeOperation<'request> {
    pub(crate) fn new(request: &'request CompositeTransactionRequest) -> Result<Self, GfError> {
        // Preserve publication's fingerprint-before-envelope error precedence.
        // Frozen participant ledger construction validates its own resource limits.
        let fingerprint = request.canonical_fingerprint()?;
        Ok(Self {
            request,
            fingerprint,
            generation_uuid: composite_generation_uuid(request.request_identity().0, fingerprint),
            receipt: OnceCell::new(),
        })
    }

    pub(crate) const fn request(&self) -> &CompositeTransactionRequest {
        self.request
    }

    pub(crate) const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    pub(crate) const fn generation_uuid(&self) -> Uuid {
        self.generation_uuid
    }

    pub(crate) fn retry_decision<T: Clone>(
        &self,
        prior: Option<([u8; 32], &T)>,
    ) -> Result<Option<T>, GfError> {
        match prior {
            None => Ok(None),
            Some((fingerprint, result)) if fingerprint == self.fingerprint => {
                Ok(Some(result.clone()))
            }
            Some(_) => Err(GfError::Project {
                code: ProjectErrorCode::TransactionConflict,
                message: "composite request identity reused with different canonical content"
                    .into(),
            }),
        }
    }

    pub(crate) fn receipt(&self) -> Result<RecordBatch, GfError> {
        if let Some(receipt) = self.receipt.get() {
            return Ok(receipt.clone());
        }
        let receipt = build_composite_receipt(self)?;
        // This operation is thread-local and the builder cannot recursively initialize it.
        self.receipt
            .set(receipt.clone())
            .expect("receipt initialized once");
        Ok(receipt)
    }
}
