//! Hash buckets accelerate canonical equality; row ordinals retain no key data.
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::Result;

use super::super::{Budget, add, mul, refused, table_bytes};
use super::KeySource;

pub(in crate::row_exec::optional_memory) struct Index<S> {
    buckets: HashMap<u64, Vec<usize>>,
    hasher: S,
    bytes: usize,
}

impl<S: BuildHasher> Index<S> {
    pub(in crate::row_exec::optional_memory) fn build(
        source: KeySource<'_>,
        null_safe: bool,
        budget: &Budget,
        hasher: S,
    ) -> Result<Self> {
        Self::build_rows(source, null_safe, budget, hasher, false)
    }

    pub(in crate::row_exec::optional_memory) fn representatives(
        source: KeySource<'_>,
        null_safe: bool,
        budget: &Budget,
        hasher: S,
    ) -> Result<Self> {
        Self::build_rows(source, null_safe, budget, hasher, true)
    }

    fn build_rows(
        source: KeySource<'_>,
        null_safe: bool,
        budget: &Budget,
        hasher: S,
        distinct: bool,
    ) -> Result<Self> {
        let mut result = Self {
            buckets: HashMap::new(),
            hasher,
            bytes: 0,
        };
        if source.columns.is_empty() {
            return Ok(result);
        }
        let mut table = 0;
        for row in 0..source.len() {
            let Some(key) = source.key(row, null_safe)? else {
                continue;
            };
            let digest = result.hasher.hash_one(&key);
            drop(key);
            if distinct && result.first(source, row, source, null_safe)?.is_some() {
                continue;
            }
            if !result.buckets.contains_key(&digest)
                && result.buckets.len() == result.buckets.capacity()
            {
                // hashbrown's 7/8 load factor and power-of-two buckets. The old
                // table stays charged until try_reserve completes its rehash.
                let next = table_bytes(
                    add(result.buckets.len(), 1)?,
                    std::mem::size_of::<(u64, Vec<usize>)>(),
                )?;
                budget
                    .admit(next)
                    .map_err(|error| error.context("OptionalMatch index"))?;
                result
                    .buckets
                    .try_reserve(1)
                    .map_err(|_| refused("index table allocation failed"))?;
                budget.release(table);
                result.bytes = add(result.bytes - table, next)?;
                table = next;
            }
            let rows = result.buckets.entry(digest).or_default();
            if rows.len() == rows.capacity() {
                let capacity = rows
                    .capacity()
                    .max(2)
                    .checked_mul(2)
                    .ok_or_else(|| refused("index row capacity overflow"))?;
                let next = mul(capacity, std::mem::size_of::<usize>())?;
                budget
                    .admit(next)
                    .map_err(|error| error.context("OptionalMatch index"))?;
                let old = mul(rows.capacity(), std::mem::size_of::<usize>())?;
                rows.try_reserve_exact(capacity - rows.len())
                    .map_err(|_| refused("index row allocation failed"))?;
                budget.release(old);
                result.bytes = add(result.bytes - old, next)?;
            }
            rows.push(row);
        }
        Ok(result)
    }

    pub(in crate::row_exec::optional_memory) fn visit(
        &self,
        outer: KeySource<'_>,
        row: usize,
        inner: KeySource<'_>,
        null_safe: bool,
        mut matched: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let Some(probe) = outer.key(row, null_safe)? else {
            return Ok(());
        };
        let digest = self.hasher.hash_one(&probe);
        if let Some(rows) = self.buckets.get(&digest) {
            for &row in rows {
                // A digest is never equality: compare the same typed/null-safe
                // OptionalJoinKeys as the original owned-key HashMap. Buckets
                // append in input order, including duplicates and collisions.
                if inner.key(row, null_safe)?.as_ref() == Some(&probe) {
                    matched(row)?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::row_exec::optional_memory) fn first(
        &self,
        probe: KeySource<'_>,
        row: usize,
        source: KeySource<'_>,
        null_safe: bool,
    ) -> Result<Option<usize>> {
        let Some(key) = probe.key(row, null_safe)? else {
            return Ok(None);
        };
        if let Some(rows) = self.buckets.get(&self.hasher.hash_one(&key)) {
            for &row in rows {
                if source.key(row, null_safe)?.as_ref() == Some(&key) {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    pub(in crate::row_exec::optional_memory) fn release(self, budget: &Budget) {
        let bytes = self.bytes;
        drop(self);
        budget.release(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListArray, RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::hash::{BuildHasherDefault, Hasher};
    use std::sync::Arc;

    #[derive(Default)]
    struct Collision;
    impl Hasher for Collision {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }

    #[test]
    fn canonical_digest_collisions_preserve_duplicates_nested_values_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, true),
            Field::new(
                "nested",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
                true,
            ),
        ]));
        let batch = |ids, lists| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(ids)),
                    Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(lists)),
                ],
            )
            .unwrap()
        };
        let inner = batch(
            vec![Some(1), Some(2), Some(1), None],
            vec![
                Some(vec![Some(7)]),
                Some(vec![Some(8)]),
                Some(vec![Some(7)]),
                None,
            ],
        );
        let outer = batch(
            vec![Some(1), Some(3), None],
            vec![Some(vec![Some(7)]), Some(vec![Some(9)]), None],
        );
        let pool = Arc::new(crate::query_pool::QueryMemoryPool::new(1024 * 1024));
        let shared: Arc<dyn MemoryPool> = pool.clone();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(shared)
            .build_arc()
            .unwrap();
        let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let budget = Budget::new(&context.task_ctx());
        budget
            .admit(
                super::super::super::batch_bytes(&inner).unwrap()
                    + super::super::super::batch_bytes(&outer).unwrap(),
            )
            .unwrap();
        budget
            .index(&inner, &outer, &[(0, 0), (1, 1)], true)
            .unwrap();
        let index = Index::build(
            KeySource {
                batch: &inner,
                columns: &[0, 1],
                mapped: None,
            },
            true,
            &budget,
            BuildHasherDefault::<Collision>::default(),
        )
        .unwrap();
        assert_eq!(
            index.buckets.len(),
            1,
            "canonical digests themselves collide"
        );
        for (row, expected) in [vec![0, 2], vec![], vec![3]].iter().enumerate() {
            let mut actual = Vec::new();
            index
                .visit(
                    KeySource {
                        batch: &outer,
                        columns: &[0, 1],
                        mapped: None,
                    },
                    row,
                    KeySource {
                        batch: &inner,
                        columns: &[0, 1],
                        mapped: None,
                    },
                    true,
                    |row| {
                        actual.push(row);
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(&actual, expected);
        }
        index.release(&budget);
        let source = KeySource {
            batch: &inner,
            columns: &[0, 1],
            mapped: None,
        };
        let representatives = Index::representatives(
            source,
            true,
            &budget,
            BuildHasherDefault::<Collision>::default(),
        )
        .unwrap();
        assert_eq!(
            representatives.buckets.values().next().unwrap(),
            &[0, 1, 3],
            "duplicate full keys retain one representative even when every digest collides"
        );
        let probe = KeySource {
            batch: &outer,
            columns: &[0, 1],
            mapped: None,
        };
        for (row, expected) in [Some(0), None, Some(3)].into_iter().enumerate() {
            assert_eq!(
                representatives.first(probe, row, source, true).unwrap(),
                expected
            );
        }
        representatives.release(&budget);
        let rows = [Some(0), Some(0), None, Some(3)];
        let mapped = KeySource {
            mapped: Some(&rows),
            ..source
        };
        let index = Index::build(
            mapped,
            true,
            &budget,
            BuildHasherDefault::<Collision>::default(),
        )
        .unwrap();
        for (row, expected) in [vec![0, 1], vec![], vec![3]].iter().enumerate() {
            let mut actual = Vec::new();
            index
                .visit(probe, row, mapped, true, |row| {
                    actual.push(row);
                    Ok(())
                })
                .unwrap();
            assert_eq!(&actual, expected);
        }
        index.release(&budget);
        drop(inner);
        drop(outer);
        drop(budget);
        assert_eq!(pool.reserved(), 0);
    }
}
