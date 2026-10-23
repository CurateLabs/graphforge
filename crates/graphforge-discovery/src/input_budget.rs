//! Aggregate budgets for typed control documents, without allocating JSON.

use crate::{DiscoveryError, limit};
use serde::Serialize;

pub(crate) struct InputBudget {
    remaining: usize,
    field: &'static str,
}

impl InputBudget {
    pub(crate) const fn new(remaining: usize, field: &'static str) -> Self {
        Self { remaining, field }
    }

    pub(crate) fn account<T: Serialize>(&mut self, value: &T) -> Result<(), DiscoveryError> {
        serde_json::to_writer(&mut *self, value).map_err(|_| limit(self.field))
    }

    pub(crate) fn account_bytes(&mut self, bytes: &[u8]) -> Result<(), DiscoveryError> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(|| limit(self.field))?;
        Ok(())
    }
}

impl std::io::Write for InputBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("document budget"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
