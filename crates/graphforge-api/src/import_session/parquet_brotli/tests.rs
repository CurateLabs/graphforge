use super::*;

use graphforge_core::ProjectErrorCode;
use parquet::basic::{BrotliLevel, Compression};
use parquet::compression::{CodecOptions, create_codec};

fn shared(capacity: usize) -> Shared {
    Rc::new(RefCell::new(Budget {
        capacity,
        live: fixed_bytes(),
        failed: false,
    }))
}

fn is_limit(error: &GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

fn compressed(input: &[u8]) -> Vec<u8> {
    let mut codec = create_codec(
        Compression::BROTLI(BrotliLevel::try_new(4).unwrap()),
        &CodecOptions::default(),
    )
    .unwrap()
    .unwrap();
    let mut encoded = Vec::new();
    codec.compress(input, &mut encoded).unwrap();
    encoded
}

#[test]
fn a_denial_stays_shared_and_sticky_after_cells_are_freed() {
    let budget = shared(fixed_bytes() + 1024);
    let mut bytes = BoundedAlloc::<u8>::new(&budget);
    let cell = bytes.alloc_cell(1024);
    assert_eq!(cell.slice().len(), 1024);
    let mut integers = BoundedAlloc::<u32>::new(&budget);
    assert!(integers.alloc_cell(1).slice().is_empty());
    drop(cell);
    assert_eq!(budget.borrow().live, fixed_bytes());
    // A small request would fit after the free, but may not succeed after the
    // denial: the decoder can otherwise index a previously denied table.
    assert!(bytes.alloc_cell(1).slice().is_empty());
    assert!(integers.alloc_cell(1).slice().is_empty());
    assert!(
        BoundedAlloc::<HuffmanCode>::new(&budget)
            .alloc_cell(1)
            .slice()
            .is_empty()
    );
    assert!(is_limit(&check(&budget).unwrap_err()));
}

#[test]
fn every_allocator_denies_before_an_uncountable_request_and_returns_its_credit() {
    let budget = shared(fixed_bytes() + 1024);
    let cell = BoundedAlloc::<HuffmanCode>::new(&budget).alloc_cell(8);
    assert_eq!(
        budget.borrow().live,
        fixed_bytes() + 8 * std::mem::size_of::<HuffmanCode>()
    );
    assert!(
        BoundedAlloc::<u32>::new(&budget)
            .alloc_cell(usize::MAX)
            .slice()
            .is_empty()
    );
    drop(cell);
    assert_eq!(budget.borrow().live, fixed_bytes());
    assert!(
        BoundedAlloc::<u8>::new(&budget)
            .alloc_cell(1)
            .slice()
            .is_empty()
    );
}

#[test]
fn a_denied_constructor_never_reads_the_compressed_input() {
    struct Unread;
    impl Read for Unread {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("a denied Brotli constructor may not begin decoding");
        }
    }
    let budget = shared(fixed_bytes() + INPUT_BYTES);
    let error = decode_with_budget(Unread, &mut [], &budget, None).unwrap_err();
    assert!(is_limit(&error), "{error}");
    assert_eq!(budget.borrow().live, fixed_bytes());
}

#[test]
fn constructor_and_initial_table_denials_are_typed_and_do_not_resume_decoding() {
    let input = vec![b'x'; 256 << 10];
    let encoded = compressed(&input);
    let huffman = std::mem::size_of::<HuffmanCode>();
    for capacity in [
        fixed_bytes(),                                         // input buffer
        fixed_bytes() + INPUT_BYTES, // unchecked constructor context-map table
        fixed_bytes() + INPUT_BYTES + 1080 * huffman, // block-type trees
        fixed_bytes() + INPUT_BYTES + (1080 + 3240) * huffman, // block-length trees
        fixed_bytes() + INPUT_BYTES + (1080 + 6480) * huffman, // ring/context state
    ] {
        let budget = shared(capacity);
        let mut output = vec![0_u8; input.len()];
        let error = decode_with_budget(encoded.as_slice(), &mut output, &budget, None).unwrap_err();
        assert!(is_limit(&error), "capacity={capacity}: {error}");
        assert_eq!(
            budget.borrow().live,
            fixed_bytes(),
            "all decoder cells dropped"
        );
        assert!(budget.borrow().failed, "frees preserve the denial");
    }
}

#[test]
fn ordinary_output_is_exact_and_size_lies_cannot_grow_the_destination() {
    let input = (0..(128 << 10))
        .map(|index| u8::try_from((index * 37) % 251).unwrap())
        .collect::<Vec<_>>();
    let encoded = compressed(&input);
    let mut output = vec![0; input.len()];
    decode(&encoded, &mut output, 32 << 20).unwrap();
    assert_eq!(output, input);
    for length in [0, 1, input.len() - 1, input.len() + 1] {
        let mut output = vec![0; length];
        let error = decode(&encoded, &mut output, 32 << 20).unwrap_err();
        assert!(
            !is_limit(&error),
            "size mismatch is malformed input: {error}"
        );
        assert_eq!(output.len(), length);
    }
}

#[test]
fn supported_large_window_headers_are_admitted_by_actual_allocation() {
    let input = vec![b'g'; 64 << 10];
    let parameters = brotli::enc::BrotliEncoderParams {
        quality: 4,
        lgwin: 25,
        large_window: true,
        ..Default::default()
    };
    let mut encoded = Vec::new();
    brotli::BrotliCompress(&mut input.as_slice(), &mut encoded, &parameters).unwrap();
    let mut output = vec![0; input.len()];
    // A 32 MiB advertised window need not allocate a 32 MiB ring for a short
    // final metablock. Admit its actual request instead of imposing a header cap.
    decode(&encoded, &mut output, 8 << 20).unwrap();
    assert_eq!(output, input);
}

#[test]
fn cancellation_during_an_actual_brotli_refill_is_typed_and_releases_cells() {
    struct CancelAfterRead<'a> {
        inner: &'a [u8],
        token: &'a crate::CancellationToken,
    }
    impl Read for CancelAfterRead<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let read = self.inner.read(output)?;
            self.token.cancel();
            Ok(read)
        }
    }
    let input = vec![b'q'; 128 << 10];
    let encoded = compressed(&input);
    let token = crate::CancellationToken::new();
    let budget = shared(32 << 20);
    let mut output = vec![0; input.len()];
    let error = decode_with_budget(
        CancelAfterRead {
            inner: &encoded,
            token: &token,
        },
        &mut output,
        &budget,
        Some(&token),
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ));
    assert_eq!(budget.borrow().live, fixed_bytes());
}
