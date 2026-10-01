use std::fs;
use std::io::Cursor;
use std::process::Command;

use super::*;

struct ShortReader {
    inner: Cursor<Vec<u8>>,
    maximum: usize,
}

impl Read for ShortReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let available = buffer.len().min(self.maximum);
        self.inner.read(&mut buffer[..available])
    }
}

#[test]
fn block_fill_preserves_ordinal_and_tombstone_bytes_across_short_reads() {
    for (width, records) in [(UUID_WIDTH_USIZE, 7_usize), (TOMBSTONE_WIDTH_USIZE, 11)] {
        let expected = (0..width * records)
            .map(|value| u8::try_from(value % 251).unwrap())
            .collect::<Vec<_>>();
        let mut reader = ShortReader {
            inner: Cursor::new(expected.clone()),
            maximum: 3,
        };
        let mut actual = vec![0_u8; expected.len()];
        let mut metrics = V4OrdinalAdmissionMetrics::default();
        assert_eq!(
            read_fill_or_eof(&mut reader, &mut actual, &mut metrics).unwrap(),
            expected.len()
        );
        assert_eq!(actual, expected);
        assert_eq!(Sha256::digest(&actual), Sha256::digest(&expected));
        assert!(metrics.sequential_read_calls > 1);
        assert_eq!(metrics.authenticated_bytes, expected.len() as u64);
    }
}

struct Fixture {
    root: tempfile::TempDir,
    manifest: V4OrdinalIdentityManifest,
}

impl Fixture {
    fn new(range_sizes: &[u64], tombstones: &[u64]) -> Self {
        let root = tempfile::TempDir::new().unwrap();
        let index = root.path().join(INDEX_DIR);
        fs::create_dir_all(&index).unwrap();
        fs::write(index.join(LOCK_NAME), []).unwrap();
        let mut next = 1_u64;
        let mut ordinal_ranges = Vec::new();
        let mut mappings = Vec::new();
        for (ordinal, count) in range_sizes.iter().copied().enumerate() {
            let mut bytes = Vec::new();
            for id in next..next + count {
                let uuid = *Uuid::from_u128(id as u128).as_bytes();
                bytes.extend_from_slice(&uuid);
                mappings.push((uuid, id));
            }
            let name = format!("ordinal-{ordinal}.uuidx");
            fs::write(index.join(&name), &bytes).unwrap();
            ordinal_ranges.push(V4OrdinalRange {
                first_node_id: next,
                count,
                artifact: artifact(name, V4OrdinalArtifactKind::OrdinalUuids, 7, &bytes),
                blocks: ordinal_blocks(&bytes),
            });
            next += count + 3; // prove sparse ranges are packed, never max-id padded
        }
        let tombstone_bytes = tombstones
            .iter()
            .flat_map(|id| id.to_be_bytes())
            .collect::<Vec<_>>();
        fs::write(index.join("tombstones.uuidx"), &tombstone_bytes).unwrap();
        mappings.sort_unstable_by_key(|(uuid, _)| *uuid);
        let forward_bytes = mappings
            .iter()
            .flat_map(|(uuid, id)| uuid.iter().copied().chain(id.to_be_bytes()))
            .collect::<Vec<_>>();
        fs::write(index.join("forward.uuidx"), &forward_bytes).unwrap();
        let manifest = V4OrdinalIdentityManifest {
            format_version: ORDINAL_IDENTITY_V4,
            topology_generation: 7,
            forward_identities: vec![artifact(
                "forward.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                7,
                &forward_bytes,
            )],
            ordinal_ranges,
            tombstones: vec![V4OrdinalTombstones {
                generation: 7,
                artifact: artifact(
                    "tombstones.uuidx".into(),
                    V4OrdinalArtifactKind::NodeTombstones,
                    7,
                    &tombstone_bytes,
                ),
                blocks: tombstone_blocks(tombstones),
            }],
            uuid_order_matches_ordinals: None,
        };
        fs::write(
            index.join(MANIFEST_NAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        Self { root, manifest }
    }

    fn publish(&self) {
        fs::write(
            self.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
            serde_json::to_vec(&self.manifest).unwrap(),
        )
        .unwrap();
    }

    fn open(&self, limits: V4OrdinalIdentityLimits) -> V4OrdinalIdentityHandle {
        match V4OrdinalIdentityHandle::open(self.root.path(), &self.authority(7), limits).unwrap() {
            V4OrdinalIdentityOpen::Ready(handle) => *handle,
            V4OrdinalIdentityOpen::RebuildRequired { .. } => panic!("fixture is v4"),
        }
    }

    /// Open lazily, then prove every artifact byte and cross-artifact
    /// invariant, as a writer does before building on the artifacts.
    fn open_complete(&self, limits: V4OrdinalIdentityLimits) -> V4OrdinalIdentityHandle {
        let mut handle = self.open(limits);
        handle.admit_complete().unwrap();
        handle
    }

    /// The lazy open must succeed (it reads no artifact byte), and complete
    /// admission must refuse with `expected`.
    fn assert_complete_admission_refuses(&self, expected: &V4OrdinalIdentityError) {
        let mut handle = self.open(V4OrdinalIdentityLimits::default());
        assert_eq!(&handle.admit_complete().unwrap_err(), expected);
        // A refusal is not memoized as success.
        assert_eq!(&handle.admit_complete().unwrap_err(), expected);
    }

    fn authority(&self, topology_generation: u64) -> V4OrdinalIdentityAuthority {
        let body = fs::read(self.root.path().join(INDEX_DIR).join(MANIFEST_NAME)).unwrap();
        V4OrdinalIdentityAuthority {
            topology_generation,
            manifest_sha256: hex(&Sha256::digest(body)),
        }
    }
}

#[test]
fn retained_authenticated_handle_never_reenumerates_replaced_manifest_names() {
    let fixture = Fixture::new(&[2], &[1]);
    let handle = fixture.open(V4OrdinalIdentityLimits::default());
    let mut replacement = fixture.manifest.clone();
    replacement.forward_identities[0].name = "planted-forward.uuidx".into();
    fs::write(
        fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
        serde_json::to_vec(&replacement).unwrap(),
    )
    .unwrap();

    let referenced = handle.referenced_file_names();
    assert!(referenced.contains("forward.uuidx"));
    assert!(!referenced.contains("planted-forward.uuidx"));
}

#[test]
fn session_pin_authenticates_once_then_uses_retained_immutable_handles() {
    let fixture = Fixture::new(&[4], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let pin = handle.revalidate_for_session().unwrap();
    assert_eq!(pin.calls, 11); // root + coordination/manifest + three artifacts
    assert_eq!(pin.bytes_read, 0);

    let manifest = fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME);
    fs::write(&manifest, b"planted after the session pin").unwrap();
    let pinned = handle.lookup_node_uuids_pinned(&[1, 4]).unwrap();
    assert_eq!(pinned.metrics.revalidation_calls, 0);
    assert_eq!(pinned.metrics.revalidation_bytes, 0);
    assert!(pinned.values.iter().all(Option::is_some));

    assert_eq!(
        handle.lookup_node_uuids(&[1]).unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
}

fn artifact(
    name: String,
    kind: V4OrdinalArtifactKind,
    generation: u64,
    bytes: &[u8],
) -> V4OrdinalArtifact {
    V4OrdinalArtifact {
        name,
        kind,
        generation,
        bytes: bytes.len() as u64,
        sha256: hex(&Sha256::digest(bytes)),
        xxh64: crate::corruption_checksum::checksum(bytes),
    }
}

fn tombstone_blocks(ids: &[u64]) -> Vec<V4OrdinalTombstoneBlock> {
    ids.chunks((TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH) as usize)
        .enumerate()
        .map(|(index, chunk)| V4OrdinalTombstoneBlock {
            offset: index as u64 * TOMBSTONE_BLOCK_BYTES,
            count: chunk.len() as u64,
            first: chunk[0],
            last: chunk[chunk.len() - 1],
            xxh64: crate::corruption_checksum::checksum(
                &chunk
                    .iter()
                    .flat_map(|id| id.to_be_bytes())
                    .collect::<Vec<_>>(),
            ),
        })
        .collect()
}

#[test]
fn checksum_ordinal_manifest_refuses_legacy_missing_and_malformed_metadata() {
    let fixture = Fixture::new(&[2], &[]);
    let original = serde_json::to_value(&fixture.manifest).unwrap();
    for mode in 0..9 {
        let mut changed = original.clone();
        match mode {
            0 => {
                changed["format_version"] = serde_json::json!(5);
                changed["ordinal_ranges"][0]["artifact"]
                    .as_object_mut()
                    .unwrap()
                    .remove("xxh64");
            }
            4 => {
                changed["format_version"] = serde_json::json!(7);
                changed["ordinal_ranges"][0]["artifact"]
                    .as_object_mut()
                    .unwrap()
                    .remove("xxh64");
            }
            5 => {
                changed.as_object_mut().unwrap().remove("format_version");
            }
            6 => {
                changed["ordinal_ranges"][0]["blocks"][0]["sha256"] =
                    serde_json::json!("a".repeat(64))
            }
            7 => {
                changed["tombstones"][0]["blocks"] = serde_json::json!([{ "offset":0,"count":1,"first":1,"last":1,"xxh64":"0000000000000000","sha256":"a".repeat(64) }])
            }
            8 => {
                changed["ordinal_ranges"][0]["artifact"]["xxh64"] =
                    serde_json::json!("00000000000000000")
            }
            1 => {
                changed["ordinal_ranges"][0]["artifact"]
                    .as_object_mut()
                    .unwrap()
                    .remove("xxh64");
            }
            2 => {
                changed["ordinal_ranges"][0]["blocks"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("xxh64");
            }
            _ => {
                changed["ordinal_ranges"][0]["blocks"][0]["xxh64"] =
                    serde_json::json!("not-a-checksum")
            }
        }
        assert!(
            parse_manifest(
                &serde_json::to_vec(&changed).unwrap(),
                fixture.manifest.topology_generation
            )
            .is_err(),
            "mode={mode}"
        );
    }
    assert!(
        parse_manifest(
            &serde_json::to_vec(&original).unwrap(),
            fixture.manifest.topology_generation
        )
        .unwrap()
        .is_some()
    );
}

fn ordinal_blocks(bytes: &[u8]) -> Vec<V4OrdinalBlock> {
    bytes
        .chunks(ORDINAL_BLOCK_BYTES_USIZE)
        .enumerate()
        .map(|(index, block)| V4OrdinalBlock {
            offset: index as u64 * ORDINAL_BLOCK_BYTES,
            count: block.len() as u64 / UUID_WIDTH,
            xxh64: crate::corruption_checksum::checksum(block),
        })
        .collect()
}

#[test]
fn shuffled_repeated_missing_and_tombstoned_lookup_is_exact() {
    let fixture = Fixture::new(&[6, 3], &[3]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let result = handle.lookup_node_uuids(&[6, 3, 1, 6, 7, 10]).unwrap();
    assert_eq!(
        result.values,
        vec![
            Some(Uuid::from_u128(6)),
            None,
            Some(Uuid::from_u128(1)),
            Some(Uuid::from_u128(6)),
            None,
            Some(Uuid::from_u128(10)),
        ]
    );
    assert_eq!(result.metrics.requested, 6);
    assert_eq!(result.metrics.unique_requested, 5);
    assert_eq!(result.metrics.found, 3);
    assert_eq!(result.metrics.tombstoned, 1);
    assert_eq!(result.metrics.per_record_seeks, 0);
}

#[test]
fn generated_lookup_orders_preserve_identity_and_linear_bounds() {
    let fixture = Fixture::new(&[96], &[7, 31, 63]);
    for rotation in 0..32 {
        let mut requested = (1..=96).step_by(3).collect::<Vec<_>>();
        requested.rotate_left(rotation);
        requested.extend([7, 31, 63, 97, requested[0]]);
        if rotation.is_multiple_of(2) {
            requested.reverse();
        }
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        let result = handle.lookup_node_uuids(&requested).unwrap();
        let expected = requested
            .iter()
            .map(|id| {
                (!matches!(*id, 7 | 31 | 63) && *id <= 96).then(|| Uuid::from_u128(u128::from(*id)))
            })
            .collect::<Vec<_>>();
        assert_eq!(result.values, expected);
        assert_eq!(result.metrics.per_record_seeks, 0);
        assert!(result.metrics.bytes_read <= 96 * UUID_WIDTH + TOMBSTONE_BLOCK_BYTES);
        assert!(result.metrics.peak_buffer_bytes <= 4 * TOMBSTONE_BLOCK_BYTES);
    }
}

#[test]
fn absent_v4_classifies_valid_v3_and_present_malformed_v4_never_falls_back() {
    let (root, _, _) = crate::uuid_membership::tests::fixture();
    fs::write(
        root.path().join("topology/generation.json"),
        b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
    )
    .unwrap();
    crate::rebuild_uuid_membership_indexes(root.path(), crate::UuidIndexBuildLimits::default())
        .unwrap();
    let index = root.path().join(INDEX_DIR);
    fs::write(index.join(LOCK_NAME), []).unwrap();
    let v4_path = index.join(MANIFEST_NAME);
    let v3_path = index.join("manifest.json");
    let v3 = fs::read(&v3_path).unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::discover(root.path(), 7).unwrap(),
        V4OrdinalIdentityDiscovery::RebuildRequired { found_version: 3 }
    ));
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            root.path(),
            &V4OrdinalIdentityAuthority {
                topology_generation: 7,
                manifest_sha256: "00".repeat(32),
            },
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::Io)
    ));
    fs::remove_file(&v3_path).unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::discover(root.path(), 7),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));
    fs::write(&v3_path, &v3).unwrap();
    let v3_manifest: serde_json::Value = serde_json::from_slice(&v3).unwrap();
    let run_name = v3_manifest["runs"][0]["identities"]["name"]
        .as_str()
        .unwrap();
    let run_path = index.join(run_name);
    let mut run = fs::read(&run_path).unwrap();
    run[0] ^= 1;
    fs::write(&run_path, run).unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::discover(root.path(), 7),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));

    fs::write(&v4_path, b"{\"format_version\":4}").unwrap();
    assert_eq!(
        V4OrdinalIdentityHandle::discover(root.path(), 7).unwrap(),
        V4OrdinalIdentityDiscovery::Present
    );
    let malformed_digest = hex(&Sha256::digest(b"{\"format_version\":4}"));
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            root.path(),
            &V4OrdinalIdentityAuthority {
                topology_generation: 7,
                manifest_sha256: malformed_digest,
            },
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_)) | Err(V4OrdinalIdentityError::Io)
    ));

    let fixture = Fixture::new(&[2], &[]);
    fixture.publish();
    let mut mixed = fixture.manifest.clone();
    mixed.ordinal_ranges[0].artifact.kind = V4OrdinalArtifactKind::ForwardIdentities;
    fs::write(
        fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
        serde_json::to_vec(&mixed).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));
}

#[test]
fn post_open_same_inode_same_length_mutation_invalidates_capability() {
    let fixture = Fixture::new(&[3], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let path = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert_eq!(
        handle.lookup_node_uuids(&[1]).unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
}

#[test]
fn pinned_generation_authority_rejects_whole_artifact_and_manifest_substitution() {
    let original = Fixture::new(&[3], &[]);
    let authority = original.authority(7);
    let replacement = Fixture::new(&[5], &[2]);
    let original_index = original.root.path().join(INDEX_DIR);
    let replacement_index = replacement.root.path().join(INDEX_DIR);
    for entry in fs::read_dir(&replacement_index).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), original_index.join(entry.file_name())).unwrap();
    }

    assert_eq!(
        V4OrdinalIdentityHandle::open(
            original.root.path(),
            &authority,
            V4OrdinalIdentityLimits::default()
        )
        .unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
}

#[cfg(unix)]
#[test]
fn cross_process_mutation_with_restored_mtime_fails_block_authentication() {
    let fixture = Fixture::new(&[3], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let artifact = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
    let reference = fixture.root.path().join("original-time");
    assert!(
        Command::new("cp")
            .args(["-p"])
            .arg(&artifact)
            .arg(&reference)
            .status()
            .unwrap()
            .success()
    );
    assert!(Command::new("sh")
        .arg("-c")
        .arg("printf '\\001' | dd of=\"$ARTIFACT\" bs=1 seek=0 conv=notrunc 2>/dev/null && touch -r \"$REFERENCE\" \"$ARTIFACT\"")
        .env("ARTIFACT", &artifact)
        .env("REFERENCE", &reference)
        .status()
        .unwrap()
        .success());
    assert_eq!(
        handle.lookup_node_uuids(&[1]).unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
}

/// Flip one byte in place: same inode, same length. The mtime is restored
/// so no stamp can notice; only the content checksum can.
fn flip_byte_in_place(path: &Path, offset: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let modified = file.metadata().unwrap().modified().unwrap();
    let mut byte = [0_u8; 1];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
    file.set_modified(modified).unwrap();
}

const RECORDS_PER_ORDINAL_BLOCK: u64 = ORDINAL_BLOCK_BYTES / UUID_WIDTH;

#[test]
fn shared_artifact_inodes_are_admitted_only_when_read_only() {
    let fixture = Fixture::new(&[2 * RECORDS_PER_ORDINAL_BLOCK], &[]);
    let artifact = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
    let store_name = fixture.root.path().join("content-store-name");
    fs::hard_link(&artifact, &store_name).unwrap();
    let open = || {
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default(),
        )
    };
    // Another name for a writable inode could rewrite it under the handle.
    assert!(matches!(
        open(),
        Err(V4OrdinalIdentityError::Authentication)
    ));
    let set_readonly = |readonly: bool| {
        let mut permissions = fs::metadata(&artifact).unwrap().permissions();
        permissions.set_readonly(readonly);
        fs::set_permissions(&artifact, permissions).unwrap();
    };
    set_readonly(true);
    let Ok(V4OrdinalIdentityOpen::Ready(mut handle)) = open() else {
        panic!("a read-only shared inode is admissible");
    };
    // A block already authenticated is held by the handle, so only a block
    // read after the flip meets it.
    let second = RECORDS_PER_ORDINAL_BLOCK + 1;
    assert!(handle.lookup_node_uuids(&[second]).unwrap().values[0].is_some());
    // Sharing is not trust: a flip through the other name is refused by
    // the block that reads it.
    set_readonly(false);
    flip_byte_in_place(&store_name, 3);
    set_readonly(true);
    assert_eq!(
        handle.lookup_node_uuids_pinned(&[1]).unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
    assert!(handle.lookup_node_uuids_pinned(&[second]).unwrap().values[0].is_some());
}

#[test]
fn an_authenticated_block_is_read_once_per_handle_within_its_budget() {
    let fixture = Fixture::new(&[4 * RECORDS_PER_ORDINAL_BLOCK], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let first = handle.lookup_node_uuids(&[1]).unwrap();
    assert_eq!(first.metrics.bytes_read, ORDINAL_BLOCK_BYTES);
    // Sixteen lookups of ids in one block, as a two-hop issues one per
    // destination, read the block once between them.
    let mut total = first.metrics.bytes_read;
    for id in 2..=17 {
        let again = handle.lookup_node_uuids(&[id]).unwrap();
        assert_eq!(again.values[0], Some(Uuid::from_u128(u128::from(id))));
        total += again.metrics.bytes_read;
    }
    assert_eq!(total, ORDINAL_BLOCK_BYTES);
    // A second block costs one more read; the first is still held.
    let far = handle
        .lookup_node_uuids(&[2 * RECORDS_PER_ORDINAL_BLOCK + 1])
        .unwrap();
    assert_eq!(far.metrics.bytes_read, ORDINAL_BLOCK_BYTES);
    assert_eq!(
        handle.lookup_node_uuids(&[1]).unwrap().metrics.bytes_read,
        0
    );
}

#[test]
fn the_ordinal_block_cache_never_exceeds_its_budget_and_evicts_oldest_first() {
    let fixture = Fixture::new(&[4 * RECORDS_PER_ORDINAL_BLOCK], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits {
        max_ordinal_cache_bytes: 2 * ORDINAL_BLOCK_BYTES as usize,
        // One block per read, so each lookup caches exactly the block it read.
        max_coalesced_read_bytes: ORDINAL_BLOCK_BYTES_USIZE,
        ..V4OrdinalIdentityLimits::default()
    });
    let block = |index: u64| index * RECORDS_PER_ORDINAL_BLOCK + 1;
    for index in 0..3 {
        handle.lookup_node_uuids(&[block(index)]).unwrap();
    }
    // Blocks 1 and 2 are held (cap two); block 0, the oldest, was evicted.
    assert_eq!(
        handle
            .lookup_node_uuids(&[block(2)])
            .unwrap()
            .metrics
            .bytes_read,
        0
    );
    assert_eq!(
        handle
            .lookup_node_uuids(&[block(1)])
            .unwrap()
            .metrics
            .bytes_read,
        0
    );
    assert_eq!(
        handle
            .lookup_node_uuids(&[block(0)])
            .unwrap()
            .metrics
            .bytes_read,
        ORDINAL_BLOCK_BYTES
    );
    assert!(handle.ordinal_cache.charged_bytes <= 2 * ORDINAL_BLOCK_BYTES as usize);

    // Zero disables the cache.
    let mut uncached = fixture.open(V4OrdinalIdentityLimits {
        max_ordinal_cache_bytes: 0,
        ..V4OrdinalIdentityLimits::default()
    });
    uncached.lookup_node_uuids(&[1]).unwrap();
    assert_eq!(
        uncached.lookup_node_uuids(&[1]).unwrap().metrics.bytes_read,
        ORDINAL_BLOCK_BYTES
    );
}

/// One range of three full blocks holding `uuids`, with a record that says
/// UUIDs ascend.
fn three_block_fixture(uuids: Vec<u128>) -> Fixture {
    let mut fixture = Fixture::new(&[3 * RECORDS_PER_ORDINAL_BLOCK], &[]);
    let bytes = uuids
        .iter()
        .flat_map(|uuid| Uuid::from_u128(*uuid).into_bytes())
        .collect::<Vec<_>>();
    let name = fixture.manifest.ordinal_ranges[0].artifact.name.clone();
    fs::write(fixture.root.path().join(INDEX_DIR).join(&name), &bytes).unwrap();
    fixture.manifest.ordinal_ranges[0].artifact =
        artifact(name, V4OrdinalArtifactKind::OrdinalUuids, 7, &bytes);
    fixture.manifest.ordinal_ranges[0].blocks = ordinal_blocks(&bytes);
    fixture.manifest.uuid_order_matches_ordinals = Some(true);
    fixture.publish();
    fixture
}

#[test]
fn a_cached_block_still_answers_to_the_recorded_order() {
    // The middle block holds an inversion; the range ends are fine. It is read
    // (and held) before anything relies on the record. Serving it from memory
    // must then refuse exactly as reading it would.
    let records = RECORDS_PER_ORDINAL_BLOCK;
    let mut uuids = (1..=3 * u128::from(records)).collect::<Vec<_>>();
    uuids.swap(records as usize + 5, records as usize + 6);
    let fixture = three_block_fixture(uuids);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    assert!(handle.lookup_node_uuids(&[records + 1]).unwrap().values[0].is_some());
    assert_eq!(handle.uuid_order_matches_ordinals(), Ok(true));
    assert_eq!(
        handle.lookup_node_uuids(&[records + 1]).unwrap_err(),
        V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal UUIDs contradict the recorded UUID order"
        )
    );
}

#[test]
fn opening_reads_no_artifact_byte_at_any_node_count() {
    // 1x, 8x and 64x nodes: open is O(descriptors), never O(nodes).
    for blocks in [1, 8, 64] {
        let fixture = Fixture::new(&[blocks * RECORDS_PER_ORDINAL_BLOCK], &[]);
        let _capture = crate::lifecycle_io::CaptureScope::install();
        let before = crate::lifecycle_io::snapshot().unwrap();
        let handle = fixture.open(V4OrdinalIdentityLimits::default());
        let opened = crate::lifecycle_io::snapshot()
            .unwrap()
            .since(&before)
            .unwrap();
        assert_eq!(opened.totals.read_bytes, 0, "{blocks} blocks");
        let metrics = handle.admission_metrics();
        assert_eq!(
            (metrics.artifacts, metrics.authenticated_bytes),
            (0, 0),
            "{blocks} blocks"
        );
    }
}

#[test]
fn flipped_ordinal_block_is_refused_by_the_first_lookup_touching_it() {
    let fixture = Fixture::new(&[3 * RECORDS_PER_ORDINAL_BLOCK], &[]);
    let artifact = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
    // One byte inside the middle block (second of three).
    flip_byte_in_place(&artifact, ORDINAL_BLOCK_BYTES + 5);
    // Open reads nothing, so it cannot notice.
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let (first, middle, last) = (
        1,
        RECORDS_PER_ORDINAL_BLOCK + 1,
        2 * RECORDS_PER_ORDINAL_BLOCK + 1,
    );
    // Untouched blocks keep answering, exactly.
    let ok = handle.lookup_node_uuids(&[first, last]).unwrap();
    assert_eq!(
        ok.values,
        vec![
            Some(Uuid::from_u128(u128::from(first))),
            Some(Uuid::from_u128(u128::from(last)))
        ]
    );
    // The corrupted block is refused before any value is returned, even
    // when the same request also names healthy blocks.
    for request in [vec![middle], vec![first, middle, last]] {
        assert_eq!(
            handle.lookup_node_uuids(&request).unwrap_err(),
            V4OrdinalIdentityError::Authentication,
            "{request:?}"
        );
    }
    // The ordering proof reads every block, so it refuses too, and does not
    // report corruption as "unordered".
    assert_eq!(
        handle.uuid_order_matches_ordinals().unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
    // Complete admission refuses as well.
    assert!(handle.admit_complete().is_err());
}

#[test]
fn flipped_tombstone_block_is_refused_by_the_first_lookup_touching_it() {
    let per_block = TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH;
    let tombstones = (1..=3 * per_block).collect::<Vec<_>>();
    // Ordinals past the last tombstone stay live and read no tombstone block.
    let fixture = Fixture::new(&[3 * per_block + 10], &tombstones);
    flip_byte_in_place(
        &fixture.root.path().join(INDEX_DIR).join("tombstones.uuidx"),
        TOMBSTONE_BLOCK_BYTES + 3,
    );
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let live = 3 * per_block + 5;
    let healthy = handle.lookup_node_uuids(&[1, live]).unwrap();
    assert_eq!(healthy.values[0], None);
    assert_eq!(healthy.values[1], Some(Uuid::from_u128(u128::from(live))));
    assert_eq!(
        handle.lookup_node_uuids(&[3 * per_block]).unwrap().values,
        [None]
    );
    for request in [vec![per_block + 1], vec![1, per_block + 1, live]] {
        assert_eq!(
            handle.lookup_node_uuids(&request).unwrap_err(),
            V4OrdinalIdentityError::Authentication,
            "{request:?}"
        );
    }
    assert!(handle.admit_complete().is_err());
}

#[test]
fn flipped_forward_run_is_refused_when_a_writer_first_builds_on_it() {
    let fixture = Fixture::new(&[RECORDS_PER_ORDINAL_BLOCK], &[]);
    // The top bit of the last UUID keeps the run sorted and the record
    // well formed, so only the whole-artifact checksum can notice.
    flip_byte_in_place(
        &fixture.root.path().join(INDEX_DIR).join("forward.uuidx"),
        (RECORDS_PER_ORDINAL_BLOCK - 1) * FORWARD_RECORD_WIDTH,
    );
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    // Forward runs have only a whole-artifact checksum and no query reads
    // them: node->UUID lookups are unaffected...
    assert!(handle.lookup_node_uuids(&[1]).unwrap().values[0].is_some());
    // ...and the first consumer, a writer planning the next generation,
    // refuses before it is handed a single byte.
    assert_eq!(
        handle.pinned_update_inputs().unwrap_err(),
        V4OrdinalIdentityError::Authentication
    );
}

#[test]
fn lookups_attribute_every_identity_control_read() {
    let fixture = Fixture::new(&[2 * RECORDS_PER_ORDINAL_BLOCK], &[1]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot().unwrap();
    let lookup = handle
        .lookup_node_uuids(&[1, 2, RECORDS_PER_ORDINAL_BLOCK + 2])
        .unwrap();
    let recorded = crate::lifecycle_io::snapshot()
        .unwrap()
        .since(&before)
        .unwrap();
    assert!(lookup.metrics.bytes_read >= 2 * ORDINAL_BLOCK_BYTES);
    assert_eq!(recorded.totals.read_bytes, lookup.metrics.bytes_read);
    assert_eq!(
        recorded.totals.read_calls,
        lookup.metrics.sequential_read_calls
    );
}

#[test]
fn many_tombstone_runs_share_one_cache_budget_and_peak_charge() {
    let mut fixture = Fixture::new(&[32], &[]);
    let index = fixture.root.path().join(INDEX_DIR);
    fixture.manifest.topology_generation = 20;
    fixture.manifest.tombstones.clear();
    for generation in 1_u64..=20 {
        let bytes = generation.to_be_bytes();
        let name = format!("tombstones-{generation}.uuidx");
        fs::write(index.join(&name), bytes).unwrap();
        fixture.manifest.tombstones.push(V4OrdinalTombstones {
            generation,
            artifact: artifact(
                name,
                V4OrdinalArtifactKind::NodeTombstones,
                generation,
                &bytes,
            ),
            blocks: tombstone_blocks(&[generation]),
        });
    }
    fixture.publish();
    let mut handle = match V4OrdinalIdentityHandle::open(
        fixture.root.path(),
        &fixture.authority(20),
        V4OrdinalIdentityLimits {
            max_tombstone_cache_bytes: TOMBSTONE_CACHE_FIXED_CHARGE
                + 2 * (TOMBSTONE_CACHE_ENTRY_CHARGE + TOMBSTONE_WIDTH_USIZE),
            ..V4OrdinalIdentityLimits::default()
        },
    )
    .unwrap()
    {
        V4OrdinalIdentityOpen::Ready(handle) => handle,
        _ => panic!("v4"),
    };
    let result = handle
        .lookup_node_uuids(&(1..=20).collect::<Vec<_>>())
        .unwrap();
    let cache_budget =
        TOMBSTONE_CACHE_FIXED_CHARGE + 2 * (TOMBSTONE_CACHE_ENTRY_CHARGE + TOMBSTONE_WIDTH_USIZE);
    assert_eq!(handle.tombstone_cache.charged_bytes, cache_budget);
    assert_eq!(handle.tombstone_cache.entries.len(), 2);
    assert_eq!(result.metrics.retained_cache_bytes, cache_budget as u64);
    assert!(result.metrics.transient_buffer_bytes > TOMBSTONE_WIDTH);
    assert!(
        result.metrics.peak_buffer_bytes
            <= 20 * REQUEST_ENTRY_CHARGE
                + cache_budget as u64
                + TOMBSTONE_CACHE_ENTRY_CHARGE as u64
                + 3 * TOMBSTONE_WIDTH
    );
}

#[test]
fn descriptor_corruption_overlap_truncation_and_substitution_fail_closed() {
    let mut fixture = Fixture::new(&[3, 2], &[]);
    fixture.manifest.ordinal_ranges[1].first_node_id = 3;
    fixture.publish();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));

    let fixture = Fixture::new(&[3], &[]);
    let path = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
    fs::write(&path, [0_u8; 15]).unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::Authentication)
    ));

    let mut fixture = Fixture::new(&[2], &[]);
    fixture.manifest.ordinal_ranges[0].first_node_id = u64::MAX;
    fixture.publish();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));
}

#[test]
fn forward_authority_mismatch_duplicate_uuid_and_surrogate_fail_closed() {
    fn publish_forward(fixture: &mut Fixture, records: &[(u128, u64)]) {
        let bytes = records
            .iter()
            .flat_map(|(uuid, id)| {
                Uuid::from_u128(*uuid)
                    .into_bytes()
                    .into_iter()
                    .chain(id.to_be_bytes())
            })
            .collect::<Vec<_>>();
        fs::write(
            fixture.root.path().join(INDEX_DIR).join("forward.uuidx"),
            &bytes,
        )
        .unwrap();
        fixture.manifest.forward_identities[0] = artifact(
            "forward.uuidx".into(),
            V4OrdinalArtifactKind::ForwardIdentities,
            7,
            &bytes,
        );
        fixture.publish();
    }

    for records in [
        vec![(1, 2), (2, 1), (3, 3)],
        vec![(1, 1), (1, 2), (3, 3)],
        vec![(1, 1), (2, 1), (3, 3)],
    ] {
        let mut fixture = Fixture::new(&[3], &[]);
        publish_forward(&mut fixture, &records);
        // Forward runs are read only by writers; the disagreement is
        // proven by complete admission, which a writer must pass first.
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        assert!(matches!(
            handle.admit_complete(),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
        assert!(matches!(
            handle.pinned_update_inputs(),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
    }
}

#[test]
fn generation_ordered_forward_runs_may_have_interleaved_uuid_keys() {
    let mut fixture = Fixture::new(&[4], &[]);
    let index = fixture.root.path().join(INDEX_DIR);
    let encode = |records: &[(u128, u64)]| {
        records
            .iter()
            .flat_map(|(uuid, id)| {
                Uuid::from_u128(*uuid)
                    .into_bytes()
                    .into_iter()
                    .chain(id.to_be_bytes())
            })
            .collect::<Vec<_>>()
    };
    let older = encode(&[(1, 1), (3, 3)]);
    let newer = encode(&[(2, 2), (4, 4)]);
    fs::write(index.join("forward-6.uuidx"), &older).unwrap();
    fs::write(index.join("forward-7.uuidx"), &newer).unwrap();
    fixture.manifest.forward_identities = vec![
        artifact(
            "forward-6.uuidx".into(),
            V4OrdinalArtifactKind::ForwardIdentities,
            6,
            &older,
        ),
        artifact(
            "forward-7.uuidx".into(),
            V4OrdinalArtifactKind::ForwardIdentities,
            7,
            &newer,
        ),
    ];
    fixture.publish();

    let _handle = fixture.open(V4OrdinalIdentityLimits::default());
}

/// Two ranges (ordinals 1..=2 and 6..=7), one tombstone, carrying `uuids` in
/// ordinal order, with a forward run that agrees with them.
fn fixture_with_ordinal_uuids(uuids: [u128; 4]) -> Fixture {
    let mut fixture = Fixture::new(&[2, 2], &[2]);
    let index = fixture.root.path().join(INDEX_DIR);
    let mut mappings = Vec::new();
    for (range, values) in fixture
        .manifest
        .ordinal_ranges
        .iter_mut()
        .zip(uuids.chunks_exact(2))
    {
        let bytes = values
            .iter()
            .flat_map(|value| Uuid::from_u128(*value).into_bytes())
            .collect::<Vec<_>>();
        fs::write(index.join(&range.artifact.name), &bytes).unwrap();
        range.artifact = artifact(
            range.artifact.name.clone(),
            V4OrdinalArtifactKind::OrdinalUuids,
            7,
            &bytes,
        );
        range.blocks = ordinal_blocks(&bytes);
        mappings.extend(
            values
                .iter()
                .enumerate()
                .map(|(offset, uuid)| (*uuid, range.first_node_id + offset as u64)),
        );
    }
    mappings.sort_unstable();
    let bytes = mappings
        .iter()
        .flat_map(|(uuid, id)| {
            Uuid::from_u128(*uuid)
                .into_bytes()
                .into_iter()
                .chain(id.to_be_bytes())
        })
        .collect::<Vec<_>>();
    fs::write(index.join("forward.uuidx"), &bytes).unwrap();
    fixture.manifest.forward_identities = vec![artifact(
        "forward.uuidx".into(),
        V4OrdinalArtifactKind::ForwardIdentities,
        7,
        &bytes,
    )];
    fixture.publish();
    fixture
}

#[test]
fn admitted_uuid_order_proof_spans_sparse_ranges_and_tombstones() {
    for (uuids, expected) in [
        ([1_u128, 2, 3, 4], true),
        ([1, 2, 4, 3], false),
        ([3, 4, 1, 2], false),
    ] {
        let fixture = fixture_with_ordinal_uuids(uuids);
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        assert_eq!(handle.uuid_order_matches_ordinals(), Ok(expected));
        // The lazy block scan and the complete admission prove one fact.
        let mut complete = fixture.open(V4OrdinalIdentityLimits::default());
        complete.admit_complete().unwrap();
        assert_eq!(complete.uuid_order_matches_ordinals(), Ok(expected));
        let lookup = handle.lookup_node_uuids(&[1, 2, 6, 7]).unwrap();
        assert!(
            lookup.values[1].is_none(),
            "tombstoned identity remains absent"
        );
        assert_eq!(lookup.values[0], Some(Uuid::from_u128(uuids[0])));
    }
}

#[test]
fn recorded_uuid_order_answers_without_reading_and_a_lie_is_refused() {
    for (uuids, ordered) in [([1_u128, 2, 3, 4], true), ([1, 2, 4, 3], false)] {
        // A truthful `false` costs no read. A truthful `true` costs the two end
        // blocks of each range, O(ranges) and never per node. Unknown costs the
        // scan. Complete admission agrees with every truthful record.
        for recorded in [Some(ordered), None] {
            let mut fixture = fixture_with_ordinal_uuids(uuids);
            fixture.manifest.uuid_order_matches_ordinals = recorded;
            fixture.publish();
            let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
            let _capture = crate::lifecycle_io::CaptureScope::install();
            let before = crate::lifecycle_io::snapshot().unwrap();
            assert_eq!(handle.uuid_order_matches_ordinals(), Ok(ordered));
            let read = crate::lifecycle_io::snapshot()
                .unwrap()
                .since(&before)
                .unwrap()
                .totals
                .read_bytes;
            match recorded {
                Some(false) => assert_eq!(read, 0, "{uuids:?}"),
                Some(true) => assert!(read <= 4 * ORDINAL_BLOCK_BYTES, "{uuids:?}: {read}"),
                None => assert!(read > 0, "{uuids:?}"),
            }
            fixture
                .open(V4OrdinalIdentityLimits::default())
                .admit_complete()
                .unwrap();
        }

        // A lie is refused. Complete admission compares it to the ordinals...
        let mut fixture = fixture_with_ordinal_uuids(uuids);
        fixture.manifest.uuid_order_matches_ordinals = Some(!ordered);
        fixture.publish();
        fixture.assert_complete_admission_refuses(&V4OrdinalIdentityError::InvalidDescriptor(
            "recorded ordinal UUID order disagrees with the ordinals",
        ));
        // ...and a handle that is asked to rely on a recorded `true` refuses it
        // when a range end it must read contradicts it (ordinals 6 and 7 hold
        // UUIDs 4 and 3, an inversion inside one block).
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        if ordered {
            assert_eq!(handle.uuid_order_matches_ordinals(), Ok(false));
        } else {
            assert_eq!(
                handle.uuid_order_matches_ordinals(),
                Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "ordinal UUIDs contradict the recorded UUID order"
                ))
            );
            // Blocks that do ascend keep answering.
            assert!(handle.lookup_node_uuids(&[1, 2]).unwrap().values[0].is_some());
        }
    }
}

#[test]
fn a_recorded_order_is_refused_when_ranges_meet_out_of_order() {
    // Each range ascends on its own; the seam between them does not. No block
    // contradicts the record, so only the boundary check can see it.
    let mut fixture = fixture_with_ordinal_uuids([3, 4, 1, 2]);
    fixture.manifest.uuid_order_matches_ordinals = Some(true);
    fixture.publish();
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let refused = || {
        V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal UUIDs contradict the recorded UUID order",
        )
    };
    assert_eq!(handle.uuid_order_matches_ordinals(), Err(refused()));
    // Not memoized as success: the fast path never proceeds on this handle.
    assert_eq!(handle.uuid_order_matches_ordinals(), Err(refused()));
    // Complete admission refuses the same lie.
    fixture.assert_complete_admission_refuses(&V4OrdinalIdentityError::InvalidDescriptor(
        "recorded ordinal UUID order disagrees with the ordinals",
    ));
}

#[test]
fn a_recorded_order_is_refused_when_adjacent_held_blocks_meet_out_of_order() {
    // Three blocks, each ascending, with the seam between the middle and last
    // inverted. The range ends are fine, so the record survives the boundary
    // check; the middle block is then read and meets its held neighbour.
    let records = u128::from(RECORDS_PER_ORDINAL_BLOCK);
    let uuids = (0..records)
        .map(|n| n + 1)
        .chain((0..records).map(|n| n + 90_000))
        .chain((0..records).map(|n| n + 50_000))
        .collect::<Vec<_>>();
    let fixture = three_block_fixture(uuids);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    assert_eq!(handle.uuid_order_matches_ordinals(), Ok(true));
    assert_eq!(
        handle
            .lookup_node_uuids(&[RECORDS_PER_ORDINAL_BLOCK + 1])
            .unwrap_err(),
        V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal UUIDs contradict the recorded UUID order"
        )
    );
}

#[test]
fn recorded_false_is_not_a_claim_a_reader_relies_on() {
    // `false` only disables the ordered fast path; it can never produce a
    // wrong order, so lookups do no extra checking.
    let mut fixture = fixture_with_ordinal_uuids([1, 2, 3, 4]);
    fixture.manifest.uuid_order_matches_ordinals = Some(false);
    fixture.publish();
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    assert_eq!(handle.uuid_order_matches_ordinals(), Ok(false));
    assert!(
        handle
            .lookup_node_uuids(&[1, 6])
            .unwrap()
            .values
            .iter()
            .all(Option::is_some)
    );
}

#[test]
fn forward_runs_reject_cross_generation_duplicates_and_noncanonical_order() {
    let mut fixture = Fixture::new(&[4], &[]);
    let index = fixture.root.path().join(INDEX_DIR);
    let encode = |records: &[(u128, u64)]| {
        records
            .iter()
            .flat_map(|(uuid, id)| {
                Uuid::from_u128(*uuid)
                    .into_bytes()
                    .into_iter()
                    .chain(id.to_be_bytes())
            })
            .collect::<Vec<_>>()
    };
    let older = encode(&[(1, 1), (3, 3)]);
    let duplicate = encode(&[(1, 1), (2, 2), (4, 4)]);
    fs::write(index.join("forward-6.uuidx"), &older).unwrap();
    fs::write(index.join("forward-7.uuidx"), &duplicate).unwrap();
    fixture.manifest.forward_identities = vec![
        artifact(
            "forward-6.uuidx".into(),
            V4OrdinalArtifactKind::ForwardIdentities,
            6,
            &older,
        ),
        artifact(
            "forward-7.uuidx".into(),
            V4OrdinalArtifactKind::ForwardIdentities,
            7,
            &duplicate,
        ),
    ];
    fixture.publish();
    fixture.assert_complete_admission_refuses(&V4OrdinalIdentityError::InvalidDescriptor(
        "forward identity UUID is repeated across generations",
    ));

    fixture.manifest.forward_identities.swap(0, 1);
    fixture.publish();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward runs are not in canonical generation order"
        ))
    ));
}

#[test]
fn request_limit_fails_before_request_allocation() {
    let fixture = Fixture::new(&[3], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits {
        max_requested: 2,
        ..V4OrdinalIdentityLimits::default()
    });
    assert_eq!(
        handle.lookup_node_uuids(&[1, 2, 3]).unwrap_err(),
        V4OrdinalIdentityError::RequestLimit {
            requested: 3,
            maximum: 2,
        }
    );

    for cache_budget in [0, TOMBSTONE_CACHE_FIXED_CHARGE - 1] {
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits {
                    max_tombstone_cache_bytes: cache_budget,
                    ..V4OrdinalIdentityLimits::default()
                }
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(
                "lookup bounds are invalid"
            ))
        ));
    }
}

#[test]
fn failure_evidence_is_typed_sanitized_and_counts_authentication() {
    let authentication = V4OrdinalIdentityError::Authentication.evidence();
    assert_eq!(authentication.kind, V4OrdinalFailureKind::Authentication);
    assert_eq!(authentication.authentication_failures, 1);

    let bounded = V4OrdinalIdentityError::RequestLimit {
        requested: 2,
        maximum: 1,
    }
    .evidence();
    assert_eq!(bounded.kind, V4OrdinalFailureKind::RequestLimit);
    assert_eq!(bounded.authentication_failures, 0);
    assert!(!format!("{bounded:?}").contains('/'));
}

#[test]
fn generation_uppercase_digest_and_unknown_tombstone_fail_closed() {
    let fixture = Fixture::new(&[3], &[]);
    assert_eq!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(8),
            V4OrdinalIdentityLimits::default()
        )
        .unwrap_err(),
        V4OrdinalIdentityError::GenerationMismatch {
            expected: 8,
            found: 7
        }
    );

    let mut uppercase = fixture.manifest.clone();
    uppercase.ordinal_ranges[0].artifact.sha256 =
        uppercase.ordinal_ranges[0].artifact.sha256.to_uppercase();
    fs::write(
        fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
        serde_json::to_vec(&uppercase).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(_))
    ));

    let fixture = Fixture::new(&[3], &[]);
    let manifest_path = fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME);
    let mut manifest = serde_json::to_value(&fixture.manifest).unwrap();
    manifest["untrusted_extension"] = serde_json::json!(true);
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits::default()
        ),
        Err(V4OrdinalIdentityError::Io)
    ));

    let mut fixture = Fixture::new(&[3], &[]);
    let unknown = 99_u64.to_be_bytes();
    fs::write(
        fixture.root.path().join(INDEX_DIR).join("tombstones.uuidx"),
        unknown,
    )
    .unwrap();
    fixture.manifest.tombstones[0].artifact = artifact(
        "tombstones.uuidx".into(),
        V4OrdinalArtifactKind::NodeTombstones,
        7,
        &unknown,
    );
    fixture.manifest.tombstones[0].blocks = tombstone_blocks(&[99]);
    fixture.publish();
    fixture.assert_complete_admission_refuses(&V4OrdinalIdentityError::InvalidDescriptor(
        "tombstone IDs are noncanonical",
    ));
}

#[test]
fn selected_tombstone_blocks_are_cached_without_full_file_rescans() {
    let count = (TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH) * 3;
    let tombstones = (1..=count).collect::<Vec<_>>();
    let fixture = Fixture::new(&[count], &tombstones);
    let mut handle = fixture.open(V4OrdinalIdentityLimits {
        max_coalesced_read_bytes: TOMBSTONE_BLOCK_BYTES as usize,
        ..V4OrdinalIdentityLimits::default()
    });
    let first = handle.lookup_node_uuids(&[1]).unwrap();
    let repeated = handle.lookup_node_uuids(&[1]).unwrap();
    let far = handle.lookup_node_uuids(&[count]).unwrap();
    assert_eq!(first.metrics.bytes_read, TOMBSTONE_BLOCK_BYTES);
    assert_eq!(repeated.metrics.bytes_read, 0);
    assert_eq!(far.metrics.bytes_read, TOMBSTONE_BLOCK_BYTES);
    assert!(
        first.metrics.bytes_read + repeated.metrics.bytes_read + far.metrics.bytes_read
            < 3 * count * TOMBSTONE_WIDTH
    );
    assert_eq!(far.metrics.per_record_seeks, 0);
}

#[test]
fn authenticated_block_read_has_no_per_record_seeks() {
    let fixture = Fixture::new(&[16], &[]);
    let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
    let result = handle
        .lookup_node_uuids(&(1..=16).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(result.metrics.sequential_read_calls, 1);
    assert_eq!(result.metrics.bytes_read, 16 * UUID_WIDTH);
    assert_eq!(result.metrics.per_record_seeks, 0);
    // The request, the tombstone cache, the read buffer, and the copy held for
    // later lookups (charged to the lookup while both exist).
    let read_buffer = 16 * UUID_WIDTH;
    let peak_bound =
        16 * REQUEST_ENTRY_CHARGE + TOMBSTONE_CACHE_FIXED_CHARGE as u64 + read_buffer + read_buffer;
    assert!(result.metrics.peak_buffer_bytes <= peak_bound);
    // And what the handle now retains is the held block, in both counters.
    assert_eq!(
        result.metrics.retained_cache_bytes,
        TOMBSTONE_CACHE_FIXED_CHARGE as u64 + read_buffer
    );
    // A repeat lookup is charged for the block it does not read.
    let again = handle
        .lookup_node_uuids(&(1..=16).collect::<Vec<_>>())
        .unwrap();
    assert_eq!(again.metrics.bytes_read, 0);
    assert!(again.metrics.peak_buffer_bytes >= 16 * REQUEST_ENTRY_CHARGE + read_buffer);
}

#[test]
fn adjacent_authenticated_blocks_coalesce_within_gap_and_read_cap() {
    let fixture = Fixture::new(&[8_200], &[]);
    let requested = [1, 4_097, 8_200];
    let mut coalesced = fixture.open(V4OrdinalIdentityLimits::default());
    let one = coalesced.lookup_node_uuids(&requested).unwrap();
    assert_eq!(one.metrics.sequential_read_calls, 1);
    assert_eq!(one.metrics.bytes_read, 8_200 * UUID_WIDTH);

    let mut split = fixture.open(V4OrdinalIdentityLimits {
        max_coalesced_read_bytes: ORDINAL_BLOCK_BYTES_USIZE,
        ..V4OrdinalIdentityLimits::default()
    });
    let three = split.lookup_node_uuids(&requested).unwrap();
    assert_eq!(three.metrics.sequential_read_calls, 3);
    assert_eq!(three.values, one.values);
    assert_eq!(three.metrics.per_record_seeks, 0);
}

#[test]
fn one_two_four_x_work_is_linear_constant_factor() {
    let mut prior_bytes = 0;
    for count in [64_u64, 128, 256] {
        let fixture = Fixture::new(&[count], &[]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits {
            max_requested: count as usize,
            ..V4OrdinalIdentityLimits::default()
        });
        let result = handle
            .lookup_node_uuids(&(1..=count).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(result.metrics.bytes_read, count * UUID_WIDTH);
        assert_eq!(result.metrics.sequential_read_calls, 1);
        assert_eq!(result.metrics.per_record_seeks, 0);
        if prior_bytes != 0 {
            assert_eq!(result.metrics.bytes_read, prior_bytes * 2);
        }
        prior_bytes = result.metrics.bytes_read;
    }
}

#[test]
fn admission_streams_artifacts_with_bounded_cross_run_validation_counters() {
    let mut prior_bytes = 0;
    let mut prior_calls = 0;
    let mut prior_metadata = 0;
    for (count, expected_calls) in [(4_096_u64, 3_u64), (8_192, 4), (16_384, 6)] {
        let fixture = Fixture::new(&[count], &[]);
        // Opening reads no artifact byte at any size: the node-count axis.
        let lazy = fixture
            .open(V4OrdinalIdentityLimits::default())
            .admission_metrics();
        assert_eq!(
            (
                lazy.artifacts,
                lazy.authenticated_bytes,
                lazy.sequential_read_calls
            ),
            (0, 0, 0)
        );
        let handle = fixture.open_complete(V4OrdinalIdentityLimits::default());
        let metrics = handle.admission_metrics();
        assert_eq!(metrics.artifacts, 3);
        assert_eq!(
            metrics.authenticated_bytes,
            count * (FORWARD_RECORD_WIDTH * 2 + UUID_WIDTH)
        );
        assert_eq!(metrics.sequential_read_calls, expected_calls);
        assert!(metrics.peak_buffer_bytes <= STREAM_BYTES as u64);
        assert!(metrics.peak_buffer_bytes >= metrics.retained_descriptor_bytes);
        assert!(metrics.manifest_bytes > 0);
        if prior_bytes != 0 {
            assert_eq!(metrics.authenticated_bytes, prior_bytes * 2);
            assert!(metrics.sequential_read_calls <= prior_calls * 2);
            assert!(metrics.retained_descriptor_bytes >= prior_metadata);
            assert!(metrics.retained_descriptor_bytes <= prior_metadata * 2);
        }
        prior_bytes = metrics.authenticated_bytes;
        prior_calls = metrics.sequential_read_calls;
        prior_metadata = metrics.retained_descriptor_bytes;
    }
}

#[test]
fn descriptor_metadata_budget_is_enforced_before_artifact_admission() {
    let fixture = Fixture::new(&[8_192], &[]);
    let admitted = fixture.open(V4OrdinalIdentityLimits::default());
    let required = admitted.admission_metrics().retained_descriptor_bytes as usize;
    assert!(required > DESCRIPTOR_FIXED_CHARGE);
    assert!(matches!(
        V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(7),
            V4OrdinalIdentityLimits {
                max_descriptor_metadata_bytes: required - 1,
                ..V4OrdinalIdentityLimits::default()
            }
        ),
        Err(V4OrdinalIdentityError::InvalidDescriptor(
            "descriptor metadata exceeds admission bound"
        ))
    ));
}

#[test]
fn construction_metadata_rejects_disjoint_ranges_reusing_one_artifact() {
    let fixture = Fixture::new(&[1], &[]);
    let mut manifest = fixture.manifest.clone();
    let mut duplicate = manifest.ordinal_ranges[0].clone();
    duplicate.first_node_id = 2;
    manifest.ordinal_ranges.push(duplicate);
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let error =
        decode_construction_ordinal_manifest(&bytes, manifest.topology_generation).unwrap_err();
    assert_eq!(
        error,
        V4OrdinalIdentityError::InvalidDescriptor("artifact filename is reused")
    );
}

#[test]
fn tombstones_must_be_sorted_known_ordinals_but_newer_duplicates_are_safe() {
    let fixture = Fixture::new(&[4], &[2]);
    let index = fixture.root.path().join(INDEX_DIR);
    let duplicate = 2_u64.to_be_bytes();
    fs::write(index.join("tombstones-new.uuidx"), duplicate).unwrap();
    let mut manifest = fixture.manifest.clone();
    manifest.topology_generation = 8;
    manifest.tombstones.push(V4OrdinalTombstones {
        generation: 8,
        artifact: artifact(
            "tombstones-new.uuidx".into(),
            V4OrdinalArtifactKind::NodeTombstones,
            8,
            &duplicate,
        ),
        blocks: tombstone_blocks(&[2]),
    });
    // Retained range generation is allowed; newest deletion still wins.
    fs::write(
        index.join(MANIFEST_NAME),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let mut handle = match V4OrdinalIdentityHandle::open(
        fixture.root.path(),
        &fixture.authority(8),
        V4OrdinalIdentityLimits::default(),
    )
    .unwrap()
    {
        V4OrdinalIdentityOpen::Ready(handle) => *handle,
        _ => panic!("v4"),
    };
    assert_eq!(handle.lookup_node_uuids(&[2]).unwrap().values, [None]);
}
