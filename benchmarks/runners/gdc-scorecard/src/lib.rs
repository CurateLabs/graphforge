//! Converts LDBC pipe-delimited CSV (plain or gzip-compressed, named directly
//! or by wildcard) and Graphalytics `.v`/`.e` files into the
//! Parquet layout that `gf import-session register-parquet` accepts.

mod convert;
mod error;
mod glob;
mod identity;
mod mapping;
mod source;
mod spill;
mod temporal;

pub use convert::{Conversion, MANIFEST_FILE, MANIFEST_SCHEMA, convert, convert_with_budget};
pub use error::{Cause, ConvertError};
pub use identity::{edge_uuid, node_uuid};
pub use mapping::{MAPPING_SCHEMA, Mapping, TemporalFormat};
pub use temporal::{parse_date, parse_datetime};
pub use spill::{
    DEFAULT_MEMORY_BUDGET_BYTES, KEY_RECORD_BYTES, MIN_MEMORY_BUDGET_BYTES, SPILL_DIR,
};
