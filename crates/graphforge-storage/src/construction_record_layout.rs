//! Current private construction wire layout; permanent index layouts are separate.
// 12 (#900): the checkpoint omits staged allocation-ledger entries its receipt
// journal names (`staged_ledger_from_sequence`). A binary that predates it
// would read such a checkpoint as a complete, smaller ledger, so it must
// refuse the session instead.
pub(crate) const FORMAT_VERSION: u32 = 12;
// UUID, kind, retained marker, full-width surrogate.
pub(crate) const BASE_IDENTITY_WIDTH: usize = 26;
pub(crate) const IDENTITY_SURROGATE_OFFSET: usize = 18;
// Node UUID, edge UUID, endpoint role.
pub(crate) const ENDPOINT_WIDTH: usize = 33;
// Edge UUID, endpoint role, full-width node surrogate.
pub(crate) const RESOLVED_ENDPOINT_WIDTH: usize = 25;
pub(crate) const RESOLVED_SURROGATE_OFFSET: usize = 17;
