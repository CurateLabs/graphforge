//! Test-only access to production sizing and pre-existing routing controls.

pub(crate) use super::bulk::test_support::{
    ForcedPartitions, ForcedPropertyFrames, decode_pool, derived_concurrency,
    max_task_decode_bytes, property_fan_in, scratch_minimum_bytes,
};
