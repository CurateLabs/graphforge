//! Converts LDBC pipe-delimited CSV and Graphalytics `.v`/`.e` files into the
//! Parquet layout that `gf import-session register-parquet` accepts.

mod convert;
mod error;
mod identity;
mod mapping;
mod source;

pub use convert::{Conversion, MANIFEST_FILE, MANIFEST_SCHEMA, convert};
pub use error::{Cause, ConvertError};
pub use identity::{edge_uuid, node_uuid};
pub use mapping::{MAPPING_SCHEMA, Mapping};
