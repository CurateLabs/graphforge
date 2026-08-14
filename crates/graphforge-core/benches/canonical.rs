//! Benchmarks for the `graphforge-canonical/1` byte primitives (#canonical).
//!
//! Every deterministic identifier in GraphForge — assertion IDs, provenance
//! events, projection fingerprints — is produced by encoding a record with
//! [`CanonicalWriter`], hashing the domain-separated preimage, and projecting
//! the digest into a UUIDv8. That path runs once per written row, so it is on
//! the hot path of every bulk ingest.

use divan::Bencher;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalReader, CanonicalWriter, fingerprint,
    fingerprint_preimage, uuid_v8,
};
use graphforge_core::uuid::{new_v5, new_v7};

fn main() {
    divan::main();
}

/// One synthetic assertion-like record: a UUID key, two text columns, and two
/// numeric columns — the shape the knowledge tables encode per row.
fn record(index: u64) -> (u64, String, String) {
    (
        index,
        format!("graphforge/subject/{index}"),
        format!("Assertion {index} recorded by the benchmark fixture"),
    )
}

/// Encode `rows` records into one canonical payload.
fn encode_rows(rows: u64) -> Vec<u8> {
    let mut writer = CanonicalWriter::new();
    writer.u32(CANONICAL_CONTRACT_VERSION).unwrap();
    writer.u64(rows).unwrap();
    for index in 0..rows {
        let (id, subject, note) = record(index);
        writer.u64(id).unwrap();
        writer.text(&subject).unwrap();
        writer.text(&note).unwrap();
        writer.i64(-i64::try_from(index).unwrap()).unwrap();
        writer.u8(u8::try_from(index % 251).unwrap()).unwrap();
    }
    writer.finish()
}

/// Canonical encoding of a bounded record batch.
#[divan::bench(args = [16, 256, 4096])]
fn encode(bencher: Bencher, rows: u64) {
    bencher.bench(|| divan::black_box(encode_rows(divan::black_box(rows))));
}

/// Strict decoding of the same payload — the read side of the contract.
#[divan::bench(args = [16, 256, 4096])]
fn decode(bencher: Bencher, rows: u64) {
    let payload = encode_rows(rows);
    bencher.bench(|| {
        let mut reader = CanonicalReader::new(divan::black_box(&payload)).unwrap();
        let _version = reader.u32().unwrap();
        let count = reader.u64().unwrap();
        let mut bytes = 0_usize;
        for _ in 0..count {
            let _id = reader.u64().unwrap();
            bytes += reader.text().unwrap().len();
            bytes += reader.text().unwrap().len();
            let _signed = reader.u64().unwrap();
            let _tag = reader.u8().unwrap();
        }
        reader.finish().unwrap();
        divan::black_box(bytes)
    });
}

/// Domain-separated preimage framing, without the SHA-256 pass.
#[divan::bench(args = [1024, 65536])]
fn preimage(bencher: Bencher, payload_bytes: usize) {
    let payload = vec![0xA5_u8; payload_bytes];
    bencher.bench(|| {
        divan::black_box(
            fingerprint_preimage(
                CanonicalDomain::Assertion,
                CANONICAL_CONTRACT_VERSION,
                divan::black_box(&payload),
            )
            .unwrap(),
        )
    });
}

/// Full fingerprint: framing plus SHA-256.
#[divan::bench(args = [1024, 65536, 1_048_576])]
fn fingerprint_sha256(bencher: Bencher, payload_bytes: usize) {
    let payload = vec![0xA5_u8; payload_bytes];
    bencher.bench(|| {
        divan::black_box(
            fingerprint(
                CanonicalDomain::Assertion,
                CANONICAL_CONTRACT_VERSION,
                divan::black_box(&payload),
            )
            .unwrap(),
        )
    });
}

/// End-to-end deterministic identifier: encode → fingerprint → UUIDv8.
#[divan::bench(args = [16, 256])]
fn identity_pipeline(bencher: Bencher, rows: u64) {
    bencher.bench(|| {
        let payload = encode_rows(divan::black_box(rows));
        let digest = fingerprint(
            CanonicalDomain::Assertion,
            CANONICAL_CONTRACT_VERSION,
            &payload,
        )
        .unwrap();
        divan::black_box(uuid_v8(digest))
    });
}

/// UUID helpers used at the storage boundary.
mod uuid {
    use super::{Bencher, new_v5, new_v7};

    #[divan::bench]
    fn v5_name_based(bencher: Bencher) {
        let namespace = ::uuid::Uuid::NAMESPACE_URL;
        let name = b"graphforge/benchmark/node/4711";
        bencher.bench(|| divan::black_box(new_v5(divan::black_box(&namespace), name)));
    }

    #[divan::bench]
    fn v7_time_ordered(bencher: Bencher) {
        bencher.bench(|| divan::black_box(new_v7()));
    }
}
