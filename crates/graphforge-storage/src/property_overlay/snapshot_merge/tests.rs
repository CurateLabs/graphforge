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
