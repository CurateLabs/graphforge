//! Converts LDBC pipe-delimited CSV and Graphalytics `.v`/`.e` files into the
//! Parquet layout that `gf import-session register-parquet` accepts.

mod convert;
mod error;
mod identity;
mod mapping;
mod source;
mod spill;

pub use convert::{Conversion, MANIFEST_FILE, MANIFEST_SCHEMA, convert, convert_with_budget};
pub use error::{Cause, ConvertError};
pub use identity::{edge_uuid, node_uuid};
pub use mapping::{MAPPING_SCHEMA, Mapping};
pub use spill::{
    DEFAULT_MEMORY_BUDGET_BYTES, KEY_RECORD_BYTES, MIN_MEMORY_BUDGET_BYTES, SPILL_DIR,
};
