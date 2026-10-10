//! Test-only access to production sizing and pre-existing routing controls.

pub(crate) use super::bulk::test_support::{
    ForcedPartitions, ForcedPropertyFrames, decode_pool, derived_concurrency, property_fan_in,
    scratch_minimum_bytes, task_decode_bytes_bounds,
};
