use super::super::*;
use super::*;
#[test]
fn revert_identity_matches_frozen_golden_vector() {
    let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000003").unwrap();
    let checkpoint = Uuid::parse_str("4084179c-38db-8b6b-9b6e-c0b0a855e002").unwrap();
    let source = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000b0").unwrap();
    let prior = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000d0").unwrap();
    let actor = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000aa").unwrap();
    let source_digest = [0x11; 32];
    let request_digest = revert_request_digest(
        operation,
        "Release 1.0",
        checkpoint,
        source,
        source_digest,
        "restore release candidate",
        Some(actor),
    );
    assert_eq!(
        hex(&request_digest),
        "dff3755629942d1189060b117cb70dc864428fcc3b28d5c2d22d3924c3690e93"
    );
    let transaction = revert_transaction_uuid(operation);
    assert_eq!(
        transaction.to_string(),
        "908d637b-d6e6-8508-919e-d4e708e037b2"
    );
    assert_eq!(
        restoration_uuid(operation, request_digest).to_string(),
        "9e1f160c-badb-80c9-beaa-ae580910bf8a"
    );
    assert_eq!(
        restored_generation_uuid(
            transaction,
            checkpoint,
            source,
            source_digest,
            prior,
            1_720_000_000_123_456,
            request_digest,
        )
        .to_string(),
        "5dc02888-2064-8892-a0e1-2c00968ba0cc"
    );
}
