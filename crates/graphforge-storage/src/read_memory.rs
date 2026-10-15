//! Prepaid ownership at the storage/execution boundary.
//!
//! Admission uses the caller's existing DataFusion consumer and pool. Sealing
//! splits already-reserved credit; Arrow's infallible claim hook never admits
//! an allocation. Buffer tokens survive slices and clones. The batch owner
//! explicitly retains bufferless arrays and construction headers; arbitrary
//! public Arrow header clones are outside that internal ownership contract.

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;

/// Admission supplied by a first-party execution caller, using its query pool.
pub trait ReadAdmission: Send + Sync {
    /// Reserve before the protected allocation. Refusal preserves older owners.
    ///
    /// # Errors
    /// Returns the caller's resource refusal or cancellation error.
    fn reserve(&self, bytes: usize) -> Result<ReadGrant>;
    /// Check the caller's cancellation authority before synchronous read work.
    ///
    /// # Errors
    /// Returns the caller's cancellation error.
    fn check_cancelled(&self) -> Result<()>;
}

/// Owned credit extracted from an existing query reservation.
#[derive(Debug)]
pub struct ReadGrant(MemoryReservation);
impl ReadGrant {
    /// Transfer already-funded credit without releasing or re-admitting it.
    pub fn from_reserved(reservation: MemoryReservation) -> Self {
        Self(reservation)
    }

    pub(crate) fn size(&self) -> usize {
        self.0.size()
    }

    pub(crate) fn shrink_to(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.size() {
            return Err(exceeded());
        }
        self.0.shrink(self.size() - bytes);
        Ok(())
    }
}

/// Internal schema construction credit transferred alongside a prepaid batch.
/// Payload buffers never hold this owner. A producer retains it with the
/// batch through its internal handoff, including bufferless results.
#[derive(Debug)]
pub struct ReadSchemaOwner {
    _parsed: [Option<ReadGrant>; 4],
    _constructed: Option<ReadGrant>,
}

/// An immutable batch whose physical buffers already own query credit.
///
/// Only storage may seal one. Borrowing/cloning its Arrow data preserves the
/// buffer tokens; callers must keep this owner for internal wrapper metadata.
#[derive(Debug)]
pub struct PrepaidBatch {
    batch: RecordBatch,
    _headers: ReadGrant,
    schema_owner: ReadSchemaOwner,
}
impl PrepaidBatch {
    /// Borrow the batch without a second payload sponsor.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Test a local alias against still-live source arrays, including slices.
    /// Empty dangling pointers are never used as physical identity evidence.
    pub fn funds_buffer(&self, buffer: &Buffer) -> bool {
        if buffer.capacity() == 0 {
            return false;
        }
        let mut found = false;
        for array in self.batch.columns() {
            // This owner can only be constructed after the same traversal has
            // validated the shape. Source arrays remain pinned during lookup.
            visit_buffers(array.as_ref(), &mut |source| {
                found |= source.capacity() != 0
                    && source.data_ptr() == buffer.data_ptr()
                    && source.capacity() == buffer.capacity();
                Ok(())
            })
            .expect("sealed source shape");
        }
        found
    }

    /// Drop source arrays while transferring their schema credit to an
    /// internal downstream batch owner. Live buffer aliases keep their tokens.
    pub fn into_schema_owner(self) -> ReadSchemaOwner {
        let Self {
            batch,
            _headers: headers,
            schema_owner,
        } = self;
        drop(batch);
        drop(headers);
        schema_owner
    }

    #[cfg(test)]
    pub(crate) fn seal(batch: RecordBatch, grant: ReadGrant) -> Result<Self> {
        Self::seal_with_metadata(batch, grant, std::array::from_fn(|_| None))
    }

    pub(crate) fn seal_with_metadata(
        batch: RecordBatch,
        grant: ReadGrant,
        metadata: [Option<ReadGrant>; 4],
    ) -> Result<Self> {
        Self::seal_owned(
            batch,
            grant,
            ReadSchemaOwner {
                _parsed: metadata,
                _constructed: None,
            },
        )
    }

    pub(crate) fn seal_with_schema_credit(
        batch: RecordBatch,
        grant: ReadGrant,
        schema: Option<ReadGrant>,
        parsed: Option<ReadSchemaOwner>,
    ) -> Result<Self> {
        // The first input supplies concat's DataType (including timezone Arc)
        // as well as the physical values. Keep its parsed schema authority.
        let owner = ReadSchemaOwner {
            _constructed: schema,
            ..parsed.unwrap_or(ReadSchemaOwner {
                _parsed: std::array::from_fn(|_| None),
                _constructed: None,
            })
        };
        Self::seal_owned(batch, grant, owner)
    }

    fn seal_owned(
        batch: RecordBatch,
        grant: ReadGrant,
        schema_owner: ReadSchemaOwner,
    ) -> Result<Self> {
        // Data must drop before authority on every fallible path.
        let mut grant = grant;
        let batch = batch;
        let mut immutable = 0;
        for array in batch.columns() {
            if matches!(
                array.data_type(),
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Struct(_)
                    | DataType::Utf8View
                    | DataType::BinaryView
            ) {
                return Err(exceeded()); // this first storage caller seals flat topology only
            }
            visit_buffers(array.as_ref(), &mut |buffer| {
                if buffer.capacity() == 0 && !buffer.is_empty() {
                    return Err(DataFusionError::ResourcesExhausted(
                        "prepaid storage read: cannot bound external Arrow backing".into(),
                    ));
                }
                if buffer.capacity() != 0 {
                    immutable = add(immutable, add(buffer.capacity(), token_bytes())?)?;
                }
                Ok(())
            })?;
        }
        let headers = add(
            batch.get_array_memory_size().saturating_sub(
                batch
                    .columns()
                    .iter()
                    .map(Array::get_buffer_memory_size)
                    .sum(),
            ),
            add(
                std::mem::size_of::<Self>(),
                mul(batch.num_columns(), std::mem::size_of::<ArrayRef>())?,
            )?,
        )?;
        grant.shrink_to(add(immutable, headers)?)?;
        let escrow = Escrow(grant.0.split(immutable));
        for array in batch.columns() {
            visit_buffers(array.as_ref(), &mut |buffer| {
                if buffer.capacity() != 0 {
                    buffer.claim(&escrow);
                }
                Ok(())
            })?;
        }
        drop(escrow); // duplicate occurrences replace prior tokens safely
        Ok(Self {
            batch,
            _headers: grant,
            schema_owner,
        })
    }
}

#[derive(Debug)]
struct BackingLease {
    _credit: MemoryReservation,
    size: usize,
}
impl arrow_buffer::MemoryReservation for BackingLease {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        self.size = size;
    }
}
#[derive(Debug)]
struct Escrow(MemoryReservation);
impl arrow_buffer::MemoryPool for Escrow {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        Box::new(BackingLease {
            _credit: self.0.split(size + token_bytes()),
            size,
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.0.size()
    }
    fn capacity(&self) -> usize {
        self.0.size()
    }
}

pub(crate) fn token_bytes() -> usize {
    std::mem::size_of::<BackingLease>()
}
pub(crate) fn exceeded() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "prepaid storage read exceeded its admitted allocation envelope".into(),
    )
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(exceeded)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .filter(|n| isize::try_from(*n).is_ok())
        .ok_or_else(exceeded)
}

/// Borrow concrete backing buffers without allocating ArrayData or child Vecs.
/// The fallible visitor permits checked occurrence sums before any claim.
///
/// # Errors
/// Returns the visitor error or a named refusal for unsupported Arrow shapes.
pub fn visit_buffers(
    array: &dyn Array,
    visit: &mut impl FnMut(&Buffer) -> Result<()>,
) -> Result<()> {
    arrow::array::downcast_primitive_array!(array => {
        visit(array.values().inner())?;
    }
        DataType::Null => {},
        DataType::Boolean => visit(array.as_boolean().values().inner())?,
        DataType::FixedSizeBinary(_) => visit(array.as_fixed_size_binary().values())?,
        DataType::Utf8 => {
            visit(array.as_string::<i32>().offsets().inner().inner())?;
            visit(array.as_string::<i32>().values())?;
        },
        DataType::LargeUtf8 => {
            visit(array.as_string::<i64>().offsets().inner().inner())?;
            visit(array.as_string::<i64>().values())?;
        },
        DataType::Binary => {
            visit(array.as_binary::<i32>().offsets().inner().inner())?;
            visit(array.as_binary::<i32>().values())?;
        },
        DataType::LargeBinary => {
            visit(array.as_binary::<i64>().offsets().inner().inner())?;
            visit(array.as_binary::<i64>().values())?;
        },
        DataType::Utf8View => {
            visit(array.as_string_view().views().inner())?;
            for buffer in array.as_string_view().data_buffers() { visit(buffer)?; }
        },
        DataType::BinaryView => {
            visit(array.as_binary_view().views().inner())?;
            for buffer in array.as_binary_view().data_buffers() { visit(buffer)?; }
        },
        DataType::List(_) => {
            visit(array.as_list::<i32>().offsets().inner().inner())?;
            visit_buffers(array.as_list::<i32>().values().as_ref(), visit)?;
        },
        DataType::LargeList(_) => {
            visit(array.as_list::<i64>().offsets().inner().inner())?;
            visit_buffers(array.as_list::<i64>().values().as_ref(), visit)?;
        },
        DataType::FixedSizeList(_, _) => visit_buffers(array.as_fixed_size_list().values().as_ref(), visit)?,
        DataType::Struct(_) => {
            for child in array.as_struct().columns() { visit_buffers(child.as_ref(), visit)?; }
        },
        other => return Err(DataFusionError::ResourcesExhausted(format!("prepaid storage read: unsupported Arrow backing shape {other}"))),
    );
    if let Some(nulls) = array.nulls() {
        visit(nulls.buffer())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
