use arrow::buffer::MutableBuffer;

use super::{mutable, mutable_envelope, round64, vector, vector_envelope};

fn native_sequence<T: Default + Clone>() {
    let width = std::mem::size_of::<T>();
    let mut values = Vec::<T>::new();
    for required in [1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 65, 129] {
        let old = values.capacity();
        let planned = vector(old, required, width, false).unwrap();
        values.resize(required, T::default());
        let actual = values.capacity() * width;
        assert_eq!(planned.retained_bytes, actual as u64);
        assert_eq!(
            planned.peak_bytes,
            (if old == values.capacity() {
                actual
            } else {
                old * width + actual
            }) as u64
        );
        let envelope = vector_envelope(0, required, width).unwrap();
        assert!(planned.retained_bytes <= envelope.retained_bytes);
        assert!(planned.peak_bytes <= envelope.peak_bytes);
    }
}

#[test]
fn native_vector_requests_match_actual_typed_resize_capacities() {
    native_sequence::<bool>();
    native_sequence::<i16>();
    native_sequence::<i32>();
    native_sequence::<i64>();
    native_sequence::<[u64; 12]>();
}

#[test]
fn exact_reservation_keeps_the_old_allocation_in_its_peak() {
    let mut values = Vec::<i32>::with_capacity(9);
    values.resize(9, 0);
    let planned = vector(values.capacity(), 17, 4, true).unwrap();
    values.reserve_exact(8);
    assert_eq!(planned.retained_bytes, (values.capacity() * 4) as u64);
    assert_eq!(planned.peak_bytes, 9 * 4 + planned.retained_bytes);
}

#[test]
fn arrow_mutable_requests_match_actual_rounding_and_growth() {
    let mut bytes = MutableBuffer::new(0);
    for required in [1, 63, 64, 65, 127, 129, 255, 257, 1_025] {
        let old = bytes.capacity();
        let planned = mutable(old, required).unwrap();
        bytes.resize(required, 0);
        assert_eq!(planned.retained_bytes, bytes.capacity() as u64);
        assert_eq!(
            planned.peak_bytes,
            (if old == bytes.capacity() {
                old
            } else {
                old + bytes.capacity()
            }) as u64
        );
        let envelope = mutable_envelope(0, required).unwrap();
        assert!(planned.retained_bytes <= envelope.retained_bytes);
        assert!(planned.peak_bytes <= envelope.peak_bytes);
    }
    assert_eq!(round64(65).unwrap(), 128);
}

#[test]
fn source_layout_overflow_is_refused_before_a_native_request() {
    assert!(vector(0, usize::MAX, 8, false).is_err());
    assert!(vector(isize::MAX as usize, 1, 8, false).is_err());
    assert!(vector(0, 1, 0, false).is_err());
    assert!(mutable(0, usize::MAX).is_err());
    assert!(round64(isize::MAX as usize).is_err());
    assert!(vector_envelope(0, usize::MAX, 8).is_err());
    assert!(mutable_envelope(0, usize::MAX).is_err());
}

#[test]
fn unchanged_buffers_do_not_count_a_fictitious_second_allocation() {
    let native = vector(256, 20, 8, false).unwrap();
    assert_eq!(native.retained_bytes, 2_048);
    assert_eq!(native.peak_bytes, native.retained_bytes);
    let arrow = mutable_envelope(128, 64).unwrap();
    assert_eq!(arrow.retained_bytes, 128);
    assert_eq!(arrow.peak_bytes, 128);
}
