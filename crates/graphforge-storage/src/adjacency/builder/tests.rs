//! Direct adjacency builder contracts.

use super::*;
use crate::GraphWriter;
use crate::adjacency::tests::{BUILD_TS, write_diamond};
use crate::adjacency::{
    AdjacencyFreshnessState, collect_adjacency_groups, csr_from_entries, inspect_adjacency_index,
    manifest_path, read_csr, read_manifest, sharded_csr_exists, validate_adjacency_index,
    validate_adjacency_index_against,
};
use graphforge_core::uuid::{Uuid, new_v7, to_bytes};
use graphforge_core::{OntologyMode, TypeId};
use tempfile::TempDir;

#[test]
fn merge_readers_share_a_budget_and_preserve_all_records() {
    for run_count in [1, 16, 63, 64, 65] {
        let dir = TempDir::new().unwrap();
        let mut spill = SpillSession::create(&dir.path().join("runs")).unwrap();
        let mut runs = Vec::new();
        let mut expected = Vec::new();
        for run in 0..run_count {
            // Long enough to refill even the single-reader buffer. Keys
            // interleave across runs; ties exercise the entire comparator.
            let entries: Vec<_> = (0..45_000_u64)
                .map(|row| (row / 3, row % 3, run as u64))
                .collect();
            let path = spill.next_run_path("EDGE", Direction::Out);
            write_keyed_run(&path, &entries, &mut spill).unwrap();
            runs.push(path);
            expected.extend(entries);
        }
        expected.sort_unstable();
        {
            let cursors = open_run_cursors(&runs).unwrap();
            assert_eq!(cursors.len(), run_count);
            let allocated: usize = cursors.iter().map(|c| c.file.capacity()).sum();
            assert!(allocated <= 1_048_576, "{run_count} readers: {allocated}");
        }
        // Exercise the compaction boundary as well as the final merge. The
        // 20-byte header and 24-byte records cross reader buffer boundaries.
        compact_keyed_runs(
            &mut runs,
            64,
            "EDGE",
            Direction::Out,
            &mut spill,
            &mut || Ok(()),
        )
        .unwrap();
        let mut position = 0;
        merge_keyed_runs(&runs, &mut || Ok(()), &mut |entry| {
            assert_eq!(Some(&entry), expected.get(position));
            position += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(position, expected.len());
    }
}

#[test]
fn spill_cursor_decodes_with_no_buffer_and_rejects_truncation() {
    let dir = TempDir::new().unwrap();
    let mut spill = SpillSession::create(&dir.path().join("runs")).unwrap();
    let path = spill.next_run_path("EDGE", Direction::Out);
    let entries = [(1, 2, 3), (4, 5, 6)];
    write_keyed_run(&path, &entries, &mut spill).unwrap();
    for capacity in [0, 1, 23, 24, 25] {
        let mut cursor = RunCursor::open(&path, capacity).unwrap();
        for entry in entries {
            assert_eq!(cursor.current, Some(entry));
            cursor.pull().unwrap();
        }
        assert_eq!(cursor.current, None);
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(20 + 24 + 23)
        .unwrap();
    let mut cursor = RunCursor::open(&path, 25).unwrap();
    assert!(cursor.pull().is_err());
}

#[test]
fn build_is_deterministic_and_stamps_pre_scan_generation() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path()); // flush -> generation 1

    let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    assert!(rows.iter().all(|r| r.topology_generation == 1));
    // KNOWS out/in + _all out/in.
    assert_eq!(rows.len(), 4);

    let knows_path = csr_path(dir.path(), "KNOWS", Direction::Out);
    let all_in_path = csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::In);
    let knows_out = std::fs::read(knows_path.with_extension("csr.json")).unwrap();
    let all_in = std::fs::read(all_in_path.with_extension("csr.json")).unwrap();

    // Rebuild: byte-identical CSR files (R-ADJ-2).
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    assert_eq!(
        std::fs::read(knows_path.with_extension("csr.json")).unwrap(),
        knows_out
    );
    assert_eq!(
        std::fs::read(all_in_path.with_extension("csr.json")).unwrap(),
        all_in
    );

    // Per-node entries are (key, edge_id)-sorted == edge-file row order.
    let csr = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
    let knows_manifest = read_manifest(dir.path())
        .unwrap()
        .into_iter()
        .find(|r| r.relation_type == "KNOWS" && r.direction == Direction::Out)
        .unwrap();
    assert_eq!(knows_manifest.node_count, csr.node_count());
    assert_eq!(knows_manifest.edge_count, 6);
    let windows: Vec<&[u64]> = csr
        .offsets
        .windows(2)
        .map(|w| &csr.edge_ids[w[0] as usize..w[1] as usize])
        .collect();
    for per_node in windows {
        assert!(
            per_node.windows(2).all(|w| w[0] <= w[1]),
            "edge ids ascending per node"
        );
    }
}

#[test]
fn private_staging_never_changes_the_reader_visible_artifact() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let prior_manifest = std::fs::read(manifest_path(dir.path())).unwrap();
    let stage = TempDir::new_in(dir.path().parent().unwrap()).unwrap();
    let mut checkpoints = 0;

    build_adjacency_index_into(dir.path(), stage.path(), BUILD_TS + 1, || {
        checkpoints += 1;
        assert_eq!(
            std::fs::read(manifest_path(dir.path())).unwrap(),
            prior_manifest
        );
        assert_eq!(
            inspect_adjacency_index(dir.path()).unwrap().state,
            AdjacencyFreshnessState::Current
        );
        Ok(())
    })
    .unwrap();
    assert!(checkpoints >= 4);
    assert!(
        validate_adjacency_index_against(dir.path(), stage.path())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn post_delete_sparse_ids_build_round_trips() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
    let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|u| {
            w.create_node(
                *u,
                graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
            )
            .unwrap()
        })
        .collect();
    for pair in uuids.windows(2) {
        w.create_edge(new_v7(), "KNOWS", &pair[0], &pair[1])
            .unwrap();
    }
    w.flush().unwrap();

    // DETACH-DELETE the middle node n2: its id becomes a gap.
    let node_set: std::collections::HashSet<[u8; 16]> =
        std::iter::once(to_bytes(&uuids[1])).collect();
    let incident = crate::incident_edge_uuids(dir.path(), &node_set).unwrap();
    let edge_set: std::collections::HashSet<[u8; 16]> = incident.into_iter().collect();
    crate::delete_nodes_and_edges(dir.path(), &node_set, &edge_set).unwrap();

    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let csr = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
    let gap = usize::try_from(ids[1]).unwrap();
    assert_eq!(
        csr.offsets[gap],
        csr.offsets[gap + 1],
        "deleted id is an empty range"
    );
    // Survivor n3 -> n4 intact.
    let n3 = usize::try_from(ids[2]).unwrap();
    let (s, e) = (csr.offsets[n3] as usize, csr.offsets[n3 + 1] as usize);
    assert_eq!(&csr.neighbor_ids[s..e], &[ids[3]]);
}

#[test]
fn exploratory_rows_group_by_rel_type_and_union_covers_all() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, BUILD_TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    let ids: Vec<u64> = [a, b, c]
        .iter()
        .map(|u| {
            w.create_node(
                *u,
                graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
            )
            .unwrap()
        })
        .collect();
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
    w.flush().unwrap();

    build_adjacency_index(dir.path(), BUILD_TS).unwrap();

    let knows = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
    assert_eq!(knows.edge_count(), 1, "decoy OWNS row excluded");
    let owns = read_csr(&csr_path(dir.path(), "OWNS", Direction::Out)).unwrap();
    assert_eq!(owns.edge_count(), 1);

    let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
    assert_eq!(all.edge_count(), 2, "union covers both rel types");
    let row = usize::try_from(ids[0]).unwrap();
    let (s, e) = (all.offsets[row] as usize, all.offsets[row + 1] as usize);
    assert_eq!(&all.edge_ids[s..e], &[1, 2], "union in edge_id order");
    assert_eq!(&all.neighbor_ids[s..e], &[ids[1], ids[2]]);
}

#[test]
fn hostile_and_reserved_stems_are_skipped_but_counted_in_union() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, BUILD_TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    for u in [a, b, c] {
        w.create_node(
            u,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
    }
    w.create_edge(new_v7(), "a/b", &a, &b).unwrap();
    w.create_edge(new_v7(), ALL_RELATIONS_STEM, &a, &c).unwrap();
    w.flush().unwrap();

    let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    // Only the union pair: both rel names are unusable as stems.
    assert!(rows.iter().all(|r| r.relation_type == ALL_RELATIONS_STEM));
    assert_eq!(rows.len(), 2);
    let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
    assert_eq!(
        all.edge_count(),
        2,
        "skipped rels still flow into the union"
    );
    assert!(
        !csr_path(dir.path(), "a/b", Direction::Out).exists(),
        "no nested path written for the separator-bearing rel name"
    );
    assert!(!csr_path(dir.path(), "a", Direction::Out).exists());
}

#[test]
fn build_failure_leaves_no_manifest() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    // Pre-create indexes/adjacency as a READ-ONLY dir so shard publication fails.
    let adj = adjacency_dir(dir.path());
    std::fs::create_dir_all(&adj).unwrap();
    let mut perms = std::fs::metadata(&adj).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&adj, perms.clone()).unwrap();

    let result = build_adjacency_index(dir.path(), BUILD_TS);
    // Restore writability so TempDir cleanup succeeds before asserting.
    perms.set_readonly(false);
    std::fs::set_permissions(&adj, perms).unwrap();

    assert!(result.is_err());
    assert!(!manifest_path(dir.path()).exists(), "manifest written last");
    assert_eq!(read_manifest(dir.path()).unwrap(), Vec::new());
}

#[test]
fn empty_project_builds_union_pair_and_manifest() {
    let dir = TempDir::new().unwrap();
    let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| {
        r.relation_type == ALL_RELATIONS_STEM
            && r.topology_generation == 0
            && r.node_count == 0
            && r.edge_count == 0
    }));
    let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
    assert_eq!(all.offsets, vec![0]);
    assert_eq!(read_manifest(dir.path()).unwrap(), rows);
}

#[test]
fn tiny_chunk_rows_spill_build_matches_csr_from_entries() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let (groups, union) = collect_adjacency_groups(
        dir.path(),
        Some(&crate::adjacency::capture_adjacency_inventory(dir.path()).unwrap()),
    )
    .unwrap();
    let expected_knows_out = csr_from_entries(groups.get("KNOWS").unwrap(), Direction::Out);
    let expected_knows_in = csr_from_entries(groups.get("KNOWS").unwrap(), Direction::In);
    let expected_all_out = csr_from_entries(&union, Direction::Out);
    let expected_all_in = csr_from_entries(&union, Direction::In);

    let options = AdjacencyBuildOptions {
        chunk_rows: 1, // force a spill run per edge
        batch_size: 1,
        spill_dir: None,
        spill_max_bytes: None,
        memory_budget_bytes: None,
        shard_max_edges: 2,
        shard_max_nodes: 2,
        merge_fan_in: 2,
    };
    let (_, metrics) = build_adjacency_index_into_with_metrics(
        dir.path(),
        dir.path(),
        BUILD_TS,
        &options,
        &mut || Ok(()),
    )
    .unwrap();
    assert_eq!(metrics.source_rows, 6);
    assert!(metrics.spill_runs >= 8);
    assert!(metrics.csr_shards >= 4);
    assert!(metrics.peak_shard_edges <= 2);
    assert!(metrics.peak_shard_nodes <= 2);
    assert!(sharded_csr_exists(&csr_path(
        dir.path(),
        "KNOWS",
        Direction::Out
    )));

    assert_eq!(
        read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap(),
        expected_knows_out
    );
    assert_eq!(
        read_csr(&csr_path(dir.path(), "KNOWS", Direction::In)).unwrap(),
        expected_knows_in
    );
    assert_eq!(
        read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap(),
        expected_all_out
    );
    assert_eq!(
        read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::In)).unwrap(),
        expected_all_in
    );
    // Spill root cleaned after success.
    assert!(
        !adjacency_dir(dir.path())
            .join(ADJACENCY_SPILL_DIR_NAME)
            .exists()
    );
    assert!(validate_adjacency_index(dir.path()).unwrap().is_empty());
}

#[test]
fn cancelled_spill_build_cleans_spill_and_leaves_prior_index() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let prior_manifest = std::fs::read(manifest_path(dir.path())).unwrap();
    let stage = TempDir::new_in(dir.path().parent().unwrap()).unwrap();
    let spill = adjacency_dir(stage.path()).join(ADJACENCY_SPILL_DIR_NAME);
    let options = AdjacencyBuildOptions {
        chunk_rows: 1,
        batch_size: 1,
        spill_dir: Some(spill.clone()),
        spill_max_bytes: None,
        memory_budget_bytes: None,
        shard_max_edges: 2,
        shard_max_nodes: 2,
        merge_fan_in: 2,
    };
    let mut checkpoints = 0usize;
    let err = build_adjacency_index_into_with_options(
        dir.path(),
        stage.path(),
        BUILD_TS + 1,
        &options,
        &mut || {
            checkpoints += 1;
            if checkpoints > 3 {
                return Err(GfError::Api {
                    code: graphforge_core::ApiErrorCode::Cancelled,
                    message: "test cancel".into(),
                });
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_CANCELLED");
    let leftover_runs = spill.exists().then(|| {
        std::fs::read_dir(&spill)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                e.path().extension().is_some_and(|ext| ext == "run")
                    || e.file_name().to_string_lossy().starts_with("build-")
            })
            .count()
    });
    assert_eq!(
        leftover_runs.unwrap_or(0),
        0,
        "spill runs / build dirs must be cleaned on cancel"
    );
    assert!(!manifest_path(stage.path()).exists());
    assert_eq!(
        std::fs::read(manifest_path(dir.path())).unwrap(),
        prior_manifest,
        "reader-visible index unchanged"
    );
    assert_eq!(
        inspect_adjacency_index(dir.path()).unwrap().state,
        AdjacencyFreshnessState::Current
    );
}

#[test]
fn spill_max_bytes_fails_closed_without_publishing_manifest() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let stage = TempDir::new_in(dir.path().parent().unwrap()).unwrap();
    let options = AdjacencyBuildOptions {
        chunk_rows: 1,
        batch_size: 1,
        spill_dir: None,
        spill_max_bytes: Some(1), // impossible for any real run file
        memory_budget_bytes: None,
        shard_max_edges: 2,
        shard_max_nodes: 2,
        merge_fan_in: 2,
    };
    let err = build_adjacency_index_into_with_options(
        dir.path(),
        stage.path(),
        BUILD_TS,
        &options,
        &mut || Ok(()),
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_RESOURCE_LIMIT");
    assert!(!manifest_path(stage.path()).exists());
    assert!(
        !adjacency_dir(stage.path())
            .join(ADJACENCY_SPILL_DIR_NAME)
            .exists()
    );
}

#[test]
fn admitted_reserved_relations_build_portable_distinct_indexes() {
    let source = TempDir::new().unwrap();
    let artifacts = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(source.path(), OntologyMode::Strict, BUILD_TS).unwrap();
    let a = new_v7();
    let b = new_v7();
    for node in [a, b] {
        writer
            .create_node(
                node,
                graphforge_value::EntityTypeId::ontology(TypeId(1)).unwrap(),
            )
            .unwrap();
    }
    let relations = ["CON", "con", r"AUX\edge"];
    for relation in relations {
        writer.create_edge(new_v7(), relation, &a, &b).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let inventory = crate::adjacency::capture_adjacency_inventory(source.path()).unwrap();
    let options = AdjacencyBuildOptions {
        chunk_rows: 1,
        ..Default::default()
    };
    let (rows, metrics) = build_adjacency_index_from_inventory(
        source.path(),
        artifacts.path(),
        Some(&inventory),
        BUILD_TS,
        &options,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(metrics.source_rows, 3);
    assert_eq!(rows.len(), 8);
    let mut names = std::collections::BTreeSet::new();
    for relation in relations {
        for direction in [Direction::Out, Direction::In] {
            let row = rows
                .iter()
                .find(|row| row.relation_type == relation && row.direction == direction)
                .unwrap();
            assert_eq!(row.edge_count, 1);
            let path = csr_path(artifacts.path(), relation, direction);
            let name = path.file_name().unwrap().to_str().unwrap();
            assert!(name.is_ascii());
            assert!(!name.contains('\\'));
            assert!(names.insert(name.to_ascii_lowercase()));
            assert_eq!(read_csr(&path).unwrap().edge_ids.len(), 1);
        }
    }
    assert!(
        crate::adjacency::validate_adjacency_index_from_inventory(
            source.path(),
            artifacts.path(),
            Some(&inventory)
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn the_lazy_rebuilds_reads_and_writes_reach_the_lifecycle_counters() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path()); // I/O before the capture; only the build is measured.

    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot();
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let region = crate::lifecycle_io::snapshot().since(&before).unwrap();
    region.validate_for_qualification().unwrap();

    // #1449: the rebuild used to read every edge table and write its spill
    // runs, CSR shards and manifest with counters blind to all of it.
    let scan = &region.phases[&crate::StorageIoPhase::ReadPathScan];
    assert!(
        scan.read_bytes > 0,
        "rebuild reads unattributed: {region:#?}"
    );
    assert!(
        scan.write_bytes > 0,
        "rebuild writes unattributed: {region:#?}"
    );
}

#[test]
fn spill_runs_are_counted_in_both_directions_of_their_lifetime() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let stage = TempDir::new().unwrap();

    let options = AdjacencyBuildOptions {
        chunk_rows: 2, // force sorted spill runs for the six diamond edges
        ..AdjacencyBuildOptions::default()
    };
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot();
    build_adjacency_index_into_with_options(
        dir.path(),
        stage.path(),
        BUILD_TS,
        &options,
        || Ok(()),
    )
    .unwrap();
    let region = crate::lifecycle_io::snapshot().since(&before).unwrap();

    let scan = &region.phases[&crate::StorageIoPhase::ReadPathScan];
    assert!(
        scan.write_bytes > 0,
        "spill writes unattributed: {region:#?}"
    );
    assert!(
        scan.read_bytes > 0,
        "edge tables or merged spill runs unattributed: {region:#?}"
    );
}

#[test]
fn a_publish_style_scope_moves_the_build_out_of_the_read_path_row() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());

    let _capture = crate::lifecycle_io::CaptureScope::install();
    let _scope = crate::lifecycle_io::PhaseScope::enter(
        crate::StorageIoPhase::EncodeWritePostwriteAuthentication,
    );
    let before = crate::lifecycle_io::snapshot();
    build_adjacency_index(dir.path(), BUILD_TS).unwrap();
    let region = crate::lifecycle_io::snapshot().since(&before).unwrap();

    assert_eq!(
        region.phases[&crate::StorageIoPhase::ReadPathScan],
        crate::PhaseIoTotals::default(),
        "scoped build leaked into read_path_scan: {region:#?}"
    );
    let encode = &region.phases[&crate::StorageIoPhase::EncodeWritePostwriteAuthentication];
    assert!(
        encode.read_bytes > 0,
        "encode reads unattributed: {region:#?}"
    );
    assert!(
        encode.write_bytes > 0,
        "encode writes unattributed: {region:#?}"
    );
}
