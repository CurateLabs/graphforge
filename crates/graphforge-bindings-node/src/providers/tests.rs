use super::*;

#[test]
fn search_vectors_reject_f32_range_loss_and_preserve_backend_validation() {
    assert_eq!(
        vector_from_input(Some(vec![1.0, -0.5])).unwrap(),
        Some(vec![1.0, -0.5])
    );
    assert_eq!(
        vector_from_input(Some(vec![f64::MAX])).unwrap_err().status,
        "ValidationError"
    );
    assert_eq!(
        vector_from_input(Some(vec![f64::from_bits(1)]))
            .unwrap_err()
            .status,
        "ValidationError"
    );

    let non_finite = vector_from_input(Some(vec![f64::NAN, f64::INFINITY]))
        .unwrap()
        .unwrap();
    assert!(non_finite[0].is_nan());
    assert!(non_finite[1].is_infinite());
}
