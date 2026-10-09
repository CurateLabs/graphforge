//! Brotli page decoding with one live allocation budget for all decoder types.
//!
//! The pinned decoder supports large windows. Refusing an allocation must be
//! sticky: several Huffman allocations are checked only by a later allocation
//! failure. A refused constructor is never allowed to start decoding.

use std::cell::RefCell;
use std::io::Read;
use std::marker::PhantomData;
use std::rc::Rc;

use brotli_decompressor::reader::DecompressorCustomAlloc;
use brotli_decompressor::{Allocator, HuffmanCode, SliceWrapper, SliceWrapperMut};
use graphforge_core::GfError;

use super::{limit, storage};

const INPUT_BYTES: usize = 8 << 10;

struct Budget {
    capacity: usize,
    live: usize,
    failed: bool,
}

type Shared = Rc<RefCell<Budget>>;

impl Budget {
    fn reserve(&mut self, bytes: usize) -> bool {
        if self.failed {
            return false;
        }
        let Some(next) = self.live.checked_add(bytes) else {
            self.failed = true;
            return false;
        };
        if next > self.capacity {
            self.failed = true;
            return false;
        }
        self.live = next;
        true
    }

    fn release(&mut self, bytes: usize) {
        // Every cell owns exactly the charge admitted before its allocation.
        self.live -= bytes;
        // Frees must not reset `failed`: unchecked empty tables must never be
        // followed by a successful allocation of another decoder type.
    }
}

#[derive(Default)]
struct Cell<T> {
    values: Vec<T>,
    charge: Option<(Shared, usize)>,
}

impl<T> Drop for Cell<T> {
    fn drop(&mut self) {
        if let Some((shared, bytes)) = self.charge.take() {
            // Free the allocation before returning its live-byte credit.
            drop(std::mem::take(&mut self.values));
            shared.borrow_mut().release(bytes);
        }
    }
}

impl<T> SliceWrapper<T> for Cell<T> {
    fn slice(&self) -> &[T] {
        &self.values
    }
}

impl<T> SliceWrapperMut<T> for Cell<T> {
    fn slice_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}

struct BoundedAlloc<T> {
    shared: Shared,
    element: PhantomData<T>,
}

impl<T> BoundedAlloc<T> {
    fn new(shared: &Shared) -> Self {
        Self {
            shared: Rc::clone(shared),
            element: PhantomData,
        }
    }
}

impl<T: Default + Clone> Allocator<T> for BoundedAlloc<T> {
    type AllocatedMemory = Cell<T>;

    fn alloc_cell(&mut self, len: usize) -> Cell<T> {
        let Some(bytes) = len.checked_mul(std::mem::size_of::<T>()) else {
            self.shared.borrow_mut().failed = true;
            return Cell::default();
        };
        if !self.shared.borrow_mut().reserve(bytes) {
            return Cell::default();
        }
        let mut values = Vec::new();
        if values.try_reserve_exact(len).is_err() {
            let mut budget = self.shared.borrow_mut();
            budget.release(bytes);
            budget.failed = true;
            return Cell::default();
        }
        let Some(actual) = values.capacity().checked_mul(std::mem::size_of::<T>()) else {
            drop(values);
            let mut budget = self.shared.borrow_mut();
            budget.release(bytes);
            budget.failed = true;
            return Cell::default();
        };
        if actual > bytes && !self.shared.borrow_mut().reserve(actual - bytes) {
            drop(values);
            self.shared.borrow_mut().release(bytes);
            return Cell::default();
        }
        if actual < bytes {
            self.shared.borrow_mut().release(bytes - actual);
        }
        values.resize(len, T::default());
        Cell {
            values,
            charge: Some((Rc::clone(&self.shared), actual)),
        }
    }

    fn free_cell(&mut self, cell: Cell<T>) {
        drop(cell);
    }
}

type Decoder<R> = DecompressorCustomAlloc<
    R,
    Cell<u8>,
    BoundedAlloc<u8>,
    BoundedAlloc<u32>,
    BoundedAlloc<HuffmanCode>,
>;

fn fixed_bytes() -> usize {
    std::mem::size_of::<Decoder<&[u8]>>()
        + std::mem::size_of::<RefCell<Budget>>()
        + 2 * std::mem::size_of::<usize>() // Rc counters
}

fn refused() -> GfError {
    limit("the Brotli page decoder exceeded its admitted live workspace")
}

fn check(shared: &Shared) -> Result<(), GfError> {
    if shared.borrow().failed {
        Err(refused())
    } else {
        Ok(())
    }
}

/// Decode into an already admitted exact output. `capacity` is workspace beside
/// the caller-owned input/output, not another full copy of the source budget.
pub(super) fn decode(input: &[u8], output: &mut [u8], capacity: usize) -> Result<(), GfError> {
    decode_cancellable(input, output, capacity, None)
}

fn check_cancel(cancellation: Option<&crate::CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(crate::CancellationToken::is_cancelled) {
        return Err(super::cancelled());
    }
    Ok(())
}

pub(super) fn decode_cancellable(
    input: &[u8],
    output: &mut [u8],
    capacity: usize,
    cancellation: Option<&crate::CancellationToken>,
) -> Result<(), GfError> {
    check_cancel(cancellation)?;
    let fixed = fixed_bytes();
    if fixed > capacity {
        return Err(refused());
    }
    let shared = Rc::new(RefCell::new(Budget {
        capacity,
        live: fixed,
        failed: false,
    }));
    decode_with_budget(input, output, &shared, cancellation)
}

/// Refills are bounded even while a compressed stream emits no output.
struct CancelRead<'a, R> {
    inner: R,
    cancellation: Option<&'a crate::CancellationToken>,
}

impl<R: Read> Read for CancelRead<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self
            .cancellation
            .is_some_and(crate::CancellationToken::is_cancelled)
        {
            return Err(std::io::Error::other(super::cancelled()));
        }
        let length = output.len().min(INPUT_BYTES);
        self.inner.read(&mut output[..length])
    }
}

fn decode_with_budget<R: Read>(
    input: R,
    output: &mut [u8],
    shared: &Shared,
    cancellation: Option<&crate::CancellationToken>,
) -> Result<(), GfError> {
    check_cancel(cancellation)?;
    let buffer = BoundedAlloc::<u8>::new(shared).alloc_cell(INPUT_BYTES);
    check(shared)?;
    let mut decoder = Decoder::new(
        CancelRead {
            inner: input,
            cancellation,
        },
        buffer,
        BoundedAlloc::new(shared),
        BoundedAlloc::new(shared),
        BoundedAlloc::new(shared),
    );
    // BrotliState's context-map constructor does not check its allocation.
    // Never call Read after its denial, even for an empty expected output.
    check(shared)?;
    let mut written = 0;
    while written < output.len() {
        check_cancel(cancellation)?;
        let end = written.saturating_add(INPUT_BYTES).min(output.len());
        let result = decoder.read(&mut output[written..end]);
        check(shared)?;
        check_cancel(cancellation)?;
        let count = result.map_err(storage)?;
        if count == 0 {
            return Err(storage(
                "Brotli page output is shorter than its stated size",
            ));
        }
        written += count;
    }
    check_cancel(cancellation)?;
    let mut extra = [0_u8; 1];
    let result = decoder.read(&mut extra);
    check(shared)?;
    check_cancel(cancellation)?;
    if result.map_err(storage)? != 0 {
        return Err(storage("Brotli page output exceeds its stated size"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "parquet_brotli/tests.rs"]
mod tests;
