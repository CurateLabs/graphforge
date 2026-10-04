use super::super::*;

#[test]
fn cancellable_capture_stops_between_payload_files() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("properties")).unwrap();
    fs::write(root.path().join("properties/A.parquet"), b"first").unwrap();
    fs::write(root.path().join("properties/B.parquet"), b"second").unwrap();
    crate::payload_digest::take_hashed_bytes();
    let mut polls = 0;
    let error = capture_graph_files_with_cancellation(root.path(), || {
        polls += 1;
        if polls == 3 {
            Err(GfError::Api {
                code: graphforge_core::ApiErrorCode::Cancelled,
                message: "verification cancelled".to_owned(),
            })
        } else {
            Ok(())
        }
    })
    .unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            ..
        }
    ));
    assert_eq!(polls, 3);
    assert_eq!(crate::payload_digest::take_hashed_bytes(), 5);
}
