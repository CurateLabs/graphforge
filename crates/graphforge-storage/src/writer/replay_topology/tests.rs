use super::*;
use crate::writer::DataType;
use crate::writer::EntityTypeId;
use crate::writer::Field;
use crate::writer::GfError;
use crate::writer::GraphWriter;
use crate::writer::OntologyMode;
use crate::writer::Path;
use crate::writer::PrimaryEntityTypeId;
use crate::writer::REPLAY_NODE_FIXED_ROW_BYTES;
use crate::writer::Schema;
use crate::writer::Uuid;
use crate::writer::fs;
use crate::writer::replay_writer_reservation;
use crate::writer::size_of;
use crate::writer::tests::TS;
use crate::writer::write_replay_overlay_streaming;
use graphforge_core::uuid::new_v7;
use tempfile::TempDir;

#[test]
fn topology_overlay_refusal_preserves_routes_and_full_width_ids() {
    use crate::graph_delta_journal::{GraphDeltaJournalLimits, ReplayEdgeRow, ReplayOverlay};
    let base = TempDir::new().unwrap();
    let left = new_v7();
    let right = new_v7();
    let first = new_v7();
    let deleted = new_v7();
    let added = new_v7();
    let left_id = u64::MAX - 2;
    let right_id = u64::MAX - 1;
    let first_id = u64::from(u32::MAX) - 1;
    let mut writer = GraphWriter::open_at(base.path(), OntologyMode::Exploratory, TS).unwrap();
    writer.next_node_id = left_id;
    writer.next_edge_id = first_id;
    writer
        .create_node(left, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_node(right, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.create_edge(first, "KNOWS", &left, &right).unwrap();
    writer.create_edge(deleted, "LIKES", &left, &right).unwrap();
    writer.flush().unwrap();
    let original = crate::graph_delta_journal::load_base_state(base.path()).unwrap();
    let edge = |uuid: Uuid, id, rel_type: &str, reversed| ReplayEdgeRow {
        edge_uuid: uuid.to_string(),
        edge_id: id,
        rel_type: rel_type.into(),
        src_uuid: if reversed { right } else { left }.to_string(),
        dst_uuid: if reversed { left } else { right }.to_string(),
        src_id: if reversed { right_id } else { left_id },
        dst_id: if reversed { left_id } else { right_id },
        created_at_micros: TS,
    };
    let mut overlay = ReplayOverlay::default();
    overlay.edges.insert(
        first.to_string(),
        Some(edge(first, first_id, "LIKES", true)),
    );
    overlay.edges.insert(deleted.to_string(), None);
    overlay.edges.insert(
        added.to_string(),
        Some(edge(added, first_id + 2, "KNOWS", false)),
    );
    let apply = |source: &Path, overlay: &ReplayOverlay| -> Result<TempDir, GfError> {
        let target = TempDir::new().unwrap();
        let (inventory, _) = crate::capture_graph_files(source)?;
        crate::graph_files::materialize_graph_tree(source, &inventory, target.path())?;
        write_replay_overlay_streaming(
            source,
            &inventory,
            target.path(),
            overlay,
            GraphDeltaJournalLimits {
                max_batch_rows: 1,
                max_replay_memory_bytes: 2 * 1024 * 1024,
                ..Default::default()
            },
        )?;
        Ok(target)
    };
    let error = apply(base.path(), &overlay).unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
    assert_eq!(
        crate::graph_delta_journal::load_base_state(base.path()).unwrap(),
        original
    );
    assert_eq!(original.node_ids[&left.to_string()], left_id);
    assert_eq!(original.node_ids[&right.to_string()], right_id);
    assert_eq!(
        original.edge_ids[&first.to_string()],
        (first_id, left_id, right_id)
    );
    assert_eq!(
        original.edges[&deleted.to_string()],
        (left.to_string(), right.to_string(), "LIKES".into())
    );
    // A hand-built topology overlay must fail before creating any target
    // authority, including for an existing edge with a changed surrogate.
    overlay
        .edges
        .get_mut(&first.to_string())
        .unwrap()
        .as_mut()
        .unwrap()
        .edge_id += 1;
    let untouched = TempDir::new().unwrap();
    let (inventory, _) = crate::capture_graph_files(base.path()).unwrap();
    let error = write_replay_overlay_streaming(
        base.path(),
        &inventory,
        untouched.path(),
        &overlay,
        Default::default(),
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
    assert_eq!(fs::read_dir(untouched.path()).unwrap().count(), 0);
}

#[test]
fn replay_node_spool_has_exact_values_byte_boundary_and_private_cleanup() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.parquet");
    let rows = (0..7)
        .map(|index| crate::graph_delta_journal::ReplayNodeRow {
            node_uuid: new_v7().to_string(),
            node_id: u64::MAX - 7 + index,
            primary_type: PrimaryEntityTypeId::decode(1).unwrap(),
            type_ids: (1..=index + 1)
                .map(|id| EntityTypeId::decode(id as u32).unwrap())
                .collect(),
            created_at_micros: 1_700_000_000_000_000 + index as i64,
            updated_at_micros: 1_700_000_000_000_010 + index as i64,
        })
        .collect::<Vec<_>>();
    let expected = replay_node_batch(&rows.iter().collect::<Vec<_>>()).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        fs::File::create(&source).unwrap(),
        expected.schema(),
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .unwrap();
    writer.write(&expected).unwrap();
    writer.close().unwrap();
    let original = fs::read(&source).unwrap();
    let open =
        || ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&source).unwrap()).unwrap();
    let mut stream =
        spool_replay_nodes(open(), directory.path(), 7, 2, REPLAY_NODE_SPOOL_LIMIT).unwrap();
    let ReplayNodeInput::Spool(reader) = &stream else {
        panic!("private stream expected")
    };
    let required = reader.get_ref().metadata().unwrap().len();
    assert!(required < 16 * 1024);
    #[cfg(unix)]
    assert_eq!(
        directory.path().read_dir().unwrap().count(),
        1,
        "Unix spool must have no pathname"
    );
    let batches = stream.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(batches.iter().all(|batch| batch.num_rows() <= 2));
    assert_eq!(
        arrow::compute::concat_batches(&expected.schema(), &batches).unwrap(),
        expected
    );
    drop(stream);
    drop(spool_replay_nodes(open(), directory.path(), 7, 2, required).unwrap());
    let rejected = spool_replay_nodes(open(), directory.path(), 7, 2, required - 1);
    assert!(matches!(rejected, Err(ref error) if error.code() == "GF_RESOURCE_LIMIT"));
    assert!(spool_replay_nodes(open(), directory.path(), 6, 2, required).is_err());
    assert_eq!(directory.path().read_dir().unwrap().count(), 1);
    assert_eq!(fs::read(&source).unwrap(), original);
}

#[test]
fn replay_writer_reservation_scales_with_columns_and_row_groups() {
    let narrow = Schema::new(vec![Field::new("id", DataType::UInt64, false)]);
    let wide = Schema::new(
        (0..128)
            .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
    );
    let one_group = replay_writer_reservation(&wide, 8, 256, 8).unwrap();
    let four_groups = replay_writer_reservation(&wide, 32, 256, 8).unwrap();
    let narrow_four_groups = replay_writer_reservation(&narrow, 32, 256, 8).unwrap();

    assert!(four_groups > one_group);
    assert!(four_groups > narrow_four_groups);
    assert!(
        four_groups - one_group
            >= 3 * 128 * size_of::<parquet::file::metadata::ColumnChunkMetaData>()
    );
}

#[test]
fn replay_node_authority_charge_tracks_owned_entries() {
    let authority = |count: usize| ReplayNodeAuthority {
        existing_overlay: (0..count)
            .map(|index| format!("existing-{index}"))
            .collect(),
        endpoint_ids: (0..count)
            .map(|index| (format!("endpoint-{index}"), index as u64))
            .collect(),
        deleted_nodes: (0..count).map(|index| format!("deleted-{index}")).collect(),
        base_rows: count,
        maximum_row_bytes: REPLAY_NODE_FIXED_ROW_BYTES,
        reader_reservation_bytes: 0,
        spool_evidence: ReplayNodeSpoolEvidence::default(),
    };
    let n = authority(64).estimated_memory();
    let two_n = authority(128).estimated_memory();
    let four_n = authority(256).estimated_memory();

    assert!(n < two_n);
    assert!(two_n < four_n);
    assert!(two_n < n * 3);
    assert!(four_n < n * 6);
}

#[test]
fn replay_zstd_context_memory_assessment() {
    assert_eq!(
        zstd::zstd_safe::version_string(),
        "1.5.7",
        "review the codec memory bound when upgrading Zstd"
    );
    let decompressor = zstd::zstd_safe::DCtx::create();
    let mut active_decoder = zstd::zstd_safe::DCtx::create();
    let mut reused = zstd::bulk::Compressor::new(1).unwrap();
    let empty_compressor_bytes = reused.context_mut().sizeof();
    let mut measurements = Vec::new();
    let mut maximum_source = 0_usize;
    for size in [
        0, 1, 7, 16, 128, 512, 513, 1024, 4096, 16384, 16385, 32768, 32769, 65536, 131072, 262144,
        1048576, 2097152, 0, 1, 16384, 513, 2097152, 7, 65536,
    ] {
        let input = (0..size)
            .map(|index| {
                // Incompressible-looking deterministic data; workspace also checked
                // with a reusable context that has retained earlier allocations.
                let mut value = index as u64 + 0x9e37_79b9_7f4a_7c15;
                value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                (value ^ (value >> 31)).to_le_bytes()[0]
            })
            .collect::<Vec<_>>();
        let mut fresh = zstd::bulk::Compressor::new(1).unwrap();
        let compressed = fresh.compress(&input).unwrap();
        assert_eq!(zstd::bulk::decompress(&compressed, size).unwrap(), input);
        let mut decoded = Vec::with_capacity(size);
        active_decoder
            .decompress(&mut decoded, &compressed)
            .unwrap();
        assert_eq!(decoded, input);
        assert!(
            active_decoder.sizeof() + empty_compressor_bytes
                <= crate::permanent_parquet::ZSTD_DECODER_WORKSPACE
        );
        let reused_output = reused.compress(&input).unwrap();
        assert_eq!(compressed, reused_output);
        // Pinned Zstd1 fast strategy: fixed contexts/workspace, at most
        // 32768 hash entries, and B + 11 * floor(B/4) token storage.
        // Retained state is bounded by the lifetime maximum source size.
        // The dormant DCtx is part of this encoder only, not an input reader.
        maximum_source = maximum_source.max(size);
        let envelope = crate::permanent_parquet::zstd_encoder_workspace(maximum_source);
        assert!(fresh.context_mut().sizeof() + decompressor.sizeof() <= envelope);
        assert!(reused.context_mut().sizeof() + decompressor.sizeof() <= envelope);

        measurements.push(serde_json::json!({"source_bytes":size, "maximum_source_bytes":maximum_source, "sampled_envelope":envelope,
                "compressed_len":compressed.len(), "compressed_capacity":compressed.capacity(),
                "fresh_compressor_bytes":fresh.context_mut().sizeof(),
                "reused_compressor_bytes":reused.context_mut().sizeof(),
                "active_decoder_bytes":active_decoder.sizeof(), "decoded_capacity":decoded.capacity()}));
    }
    println!(
        "REPLAY_ZSTD_MEMORY {}",
        serde_json::json!({
            "zstd_version":zstd::zstd_safe::version_string(),
            "empty_compressor_bytes":empty_compressor_bytes,
            "empty_decompressor_bytes":decompressor.sizeof(),
            "measurements":measurements})
    );
}
