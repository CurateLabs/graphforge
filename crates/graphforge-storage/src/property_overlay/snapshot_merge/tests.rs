use super::super::*;
use super::*;
use tempfile::TempDir;

#[test]
fn bounded_external_merge_emits_exact_newest_snapshot_and_tombstone() {
    let dir = TempDir::new().unwrap();
    let (uuid_a, uuid_b, uuid_c) = ([1; 16], [2; 16], [3; 16]);
    let inputs = vec![
        (
            PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            },
            101,
            2,
            vec![
                PropertySnapshotRow {
                    uuid: uuid_a,
                    tombstone: false,
                    values: BTreeMap::from([("name".into(), IrLiteral::Str("old".into()))]),
                },
                PropertySnapshotRow {
                    uuid: uuid_b,
                    tombstone: false,
                    values: BTreeMap::from([("keep".into(), IrLiteral::Int(1))]),
                },
            ],
        ),
        (
            PropertyFragmentId {
                generation: 2,
                ordinal: 0,
            },
            202,
            3,
            vec![
                PropertySnapshotRow {
                    uuid: uuid_a,
                    tombstone: false,
                    values: BTreeMap::from([("name".into(), IrLiteral::Str("new".into()))]),
                },
                PropertySnapshotRow {
                    uuid: uuid_c,
                    tombstone: false,
                    values: BTreeMap::new(),
                },
            ],
        ),
        (
            PropertyFragmentId {
                generation: 3,
                ordinal: 0,
            },
            303,
            4,
            vec![PropertySnapshotRow {
                uuid: uuid_b,
                tombstone: true,
                values: BTreeMap::new(),
            }],
        ),
        (
            PropertyFragmentId {
                generation: 2,
                ordinal: 1,
            },
            111,
            1,
            vec![PropertySnapshotRow {
                uuid: uuid_a,
                tombstone: false,
                values: BTreeMap::from([("name".into(), IrLiteral::Str("later-ordinal".into()))]),
            }],
        ),
    ];
    let mut rows = Vec::new();
    let budget = LiveByteBudget::new(1024);
    let metrics = visit_newest_property_snapshots(
        inputs,
        dir.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 1,
            max_open_runs: 2,
            max_buffered_bytes: 1024,
            max_row_bytes: 512,
        },
        &budget,
        |row| {
            rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.uuid).collect::<Vec<_>>(),
        vec![uuid_a, uuid_c]
    );
    assert_eq!(
        rows[0].values["name"],
        IrLiteral::Str("later-ordinal".into())
    );
    assert!(rows[1].values.is_empty());
    assert_eq!(metrics.physical_rows, 6);
    assert_eq!(metrics.physical_bytes, 717);
    assert_eq!(metrics.logical_rows, 2);
    assert_eq!(metrics.shadowed_rows, 3);
    assert_eq!(metrics.tombstones, 1);
    assert!(metrics.spill_runs >= 5);
    assert!(metrics.peak_run_references <= 3);
    assert!(metrics.merge_passes >= 2);
    // One decoded spill row or one cursor per open run, whichever is larger.
    assert_eq!(metrics.peak_buffered_rows, 2);
    assert!(metrics.peak_buffered_bytes > 33);
    assert!(metrics.peak_buffered_bytes < metrics.spill_bytes);
    assert_eq!(metrics.per_record_seeks, 0);
}

#[test]
fn rolling_fan_in_keeps_run_references_logarithmic() {
    let dir = TempDir::new().unwrap();
    let rows = (0_u16..1024)
        .map(|value| {
            let mut uuid = [0_u8; 16];
            uuid[14..].copy_from_slice(&value.to_be_bytes());
            PropertySnapshotRow {
                uuid,
                tombstone: false,
                values: BTreeMap::new(),
            }
        })
        .collect::<Vec<_>>();
    let budget = LiveByteBudget::new(1024);
    let metrics = visit_newest_property_snapshots(
        [(
            PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            },
            0,
            0,
            rows,
        )],
        dir.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 1,
            max_open_runs: 2,
            max_buffered_bytes: 1024,
            max_row_bytes: 512,
        },
        &budget,
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(metrics.physical_rows, 1024);
    assert!(metrics.spill_runs > 1024);
    assert!(metrics.peak_run_references <= 11);
    assert!(budget.peak() <= 1024);
}

#[test]
fn intermediate_merges_discard_shadowed_history() {
    let dir = TempDir::new().unwrap();
    let uuid = [7_u8; 16];
    let inputs = (0_u64..1024)
        .map(|generation| {
            (
                PropertyFragmentId {
                    generation,
                    ordinal: 0,
                },
                0,
                0,
                vec![PropertySnapshotRow {
                    uuid,
                    tombstone: false,
                    values: BTreeMap::from([(
                        "generation".into(),
                        IrLiteral::Int(i64::try_from(generation).unwrap()),
                    )]),
                }],
            )
        })
        .collect::<Vec<_>>();
    let budget = LiveByteBudget::new(1024);
    let mut emitted = Vec::new();
    let metrics = visit_newest_property_snapshots(
        inputs,
        dir.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 1,
            max_open_runs: 2,
            max_buffered_bytes: 1024,
            max_row_bytes: 512,
        },
        &budget,
        |row| {
            emitted.push(row);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].values["generation"], IrLiteral::Int(1023));
    assert_eq!(metrics.shadowed_rows, 1023);
    assert!(
        metrics.spill_bytes <= metrics.spool_input_bytes.saturating_mul(3),
        "intermediate merge output must remain linear in input: {metrics:#?}"
    );
    assert!(budget.peak() <= 1024);
}

// A finite-bit property generator: every exponent stratum, random mantissas and
// signs, plus the signed zeros and boundary values missed by ordinary decimals.
#[test]
fn finite_float_bits_survive_external_snapshot_merges() {
    let dir = TempDir::new().unwrap();
    let mut bits = vec![
        0,
        1 << 63,
        1,
        (1 << 63) | 1,
        f64::MIN_POSITIVE.to_bits() - 1,
        f64::MIN_POSITIVE.to_bits(),
        f64::MAX.to_bits(),
        (-f64::MAX).to_bits(),
        f64::from(f32::MAX).to_bits(),
        0.1_f64.to_bits(),
    ];
    let mut state = 0xf10a_7b17_5eed_cafe_u64;
    for exponent in 0..2047_u64 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mantissa = state & ((1 << 52) - 1);
        for sign in [0, 1 << 63] {
            bits.push(sign | (exponent << 52) | mantissa);
        }
    }
    for _ in 0..4096 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        if f64::from_bits(state).is_finite() {
            bits.push(state);
        }
    }
    let rows = bits
        .iter()
        .enumerate()
        .map(|(index, &bits)| {
            let mut uuid = [0_u8; 16];
            uuid[8..].copy_from_slice(&u64::try_from(index).unwrap().to_be_bytes());
            PropertySnapshotRow {
                uuid,
                tombstone: false,
                values: BTreeMap::from([("x".into(), IrLiteral::Float(f64::from_bits(bits)))]),
            }
        })
        .collect::<Vec<_>>();
    let budget = LiveByteBudget::new(4096);
    let mut read_bits = Vec::new();
    let metrics = visit_newest_property_snapshots(
        [(
            PropertyFragmentId {
                generation: 1,
                ordinal: 0,
            },
            0,
            0,
            rows,
        )],
        dir.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 17,
            max_open_runs: 2,
            max_buffered_bytes: 4096,
            max_row_bytes: 512,
        },
        &budget,
        |row| {
            let IrLiteral::Float(value) = row.values["x"] else {
                panic!("Float expected")
            };
            read_bits.push(value.to_bits());
            Ok(())
        },
    )
    .unwrap();
    assert!(metrics.merge_passes > 1, "exercise repeated JSONL decoding");
    assert_eq!(read_bits.len(), bits.len());
    for (index, (actual, expected)) in read_bits.iter().zip(&bits).enumerate() {
        assert_eq!(
            actual, expected,
            "finite bit-pattern case {index}: {expected:016x}"
        );
    }
}
