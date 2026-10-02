//! First-touch authentication of hydrated graph payloads (#1388).
//!
//! Opening a compact (V2) generation no longer reads payload bytes: the
//! manifest and route table are authenticated, and every payload is checked
//! only for presence and exact length. Hydration then hard-links each payload
//! into the private workspace, so a workspace path and its content-addressed
//! object are the same inode. Each hydration registers one *ticket* per linked
//! inode; the first reader to touch that inode through any admission
//! chokepoint ([`admit_file`], [`admit_path`]) checks the exact length and
//! required XXH64 against the ticket's manifest entry, memoized for the life of
//! the ticket. A refusal is memoized too, so every later touch refuses the
//! same way. A payload nothing ever reads is never checksummed.
//!
//! Identity, not path, is the key. Writers replace files rather than editing
//! them, so a replaced workspace path has a new inode and needs no admission,
//! while every hard link of an unadmitted inode (workspace, replay copy,
//! content store) maps to the same ticket.
//!
//! Checksums detect accidental corruption. They do not authenticate content or
//! replace the SHA-256 content-store name (ADR 0038, ADR 0049).

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, OnceLock};

use graphforge_core::GfError;

type IdentityKey = (u64, [u8; 16]);

fn key_of(identity: graphforge_filesystem::FileIdentity) -> IdentityKey {
    (identity.volume_serial, identity.file_id)
}

/// How hydration authenticates one hard-linked payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadClass {
    /// Bulk data a query reads selectively: nodes, edges, property fragments,
    /// and search segments. Checked on first touch.
    FirstTouch,
    /// Read only through readers that carry their own checksum authority:
    /// UUID-membership runs (block checksums in the manifest), CSR shards
    /// (per-shard XXH64 in the shard manifest), and delta runs (verified when
    /// replayed). Hydration neither reads nor registers them.
    SelfAuthenticating,
    /// Everything else: small metadata and sidecar files whose many readers
    /// open them by name (generation counters, label encoding, catalogs,
    /// adjacency build records). Hydration checks exact
    /// length and XXH64 as it links them, so an unclassified or unforeseen
    /// payload fails closed instead of being opened unchecked. These reads
    /// are attributed to `read_path_scan` like every other open-time decode of
    /// committed data: the hydration row stays the copy-and-verify protocol.
    Eager,
}

/// A v4 ordinal-identity artifact hydration hard-links. The identity handle
/// authenticates the blocks it reads, but it registers no ticket, so a
/// writer's capture must admit these bytes before they can name a new digest.
fn is_hard_linked_identity_artifact(relative_path: &str) -> bool {
    relative_path
        .strip_prefix("topology/uuid-membership/")
        .is_some_and(|name| {
            !name.contains('/')
                && Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension == "uuidx")
                && (name.starts_with("forward-v4-") || name.starts_with("ordinal-v4-"))
        })
}

/// Classify a payload by its workspace-relative path.
pub(crate) fn classify(relative_path: &str) -> PayloadClass {
    // The adjacency index manifest and each CSR shard manifest are the
    // authority for the shards they name, and the adjacency provider is their
    // only reader (`adjacency::read_manifest`, `ShardedCsrIndex::open`), both
    // admitting before they decode. Checked on first touch, a flipped byte is
    // refused by the query that reads it instead of being read as a stale index.
    let adjacency_manifest = relative_path.starts_with("indexes/adjacency/")
        && !relative_path.contains("/deltas/")
        && (relative_path.ends_with("/index_manifest.parquet")
            || relative_path.ends_with(".csr.json"));
    let first_touch = relative_path == "topology/nodes.parquet"
        || relative_path.starts_with("topology/nodes/")
        || relative_path.starts_with("topology/edges/")
        || relative_path.starts_with("properties/")
        || relative_path.starts_with("edge_properties/")
        || relative_path.starts_with("indexes/search/")
        || is_hard_linked_identity_artifact(relative_path)
        || adjacency_manifest;
    let self_authenticating = relative_path.starts_with("topology/uuid-membership/")
        || relative_path.starts_with("deltas/")
        || (relative_path.starts_with("indexes/adjacency/")
            && relative_path.contains(".csr.shards-")
            && Path::new(relative_path)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("csr")));
    if first_touch {
        PayloadClass::FirstTouch
    } else if self_authenticating {
        PayloadClass::SelfAuthenticating
    } else {
        PayloadClass::Eager
    }
}

/// Check an opened payload against its manifest entry now, outside any ticket,
/// returning the number of reads it took. The read is attributed to
/// `read_path_scan`, not to the hydration row: that row is the copy-and-verify
/// protocol over controls, and these are committed data read at open.
pub(crate) fn admit_now(
    file: &File,
    entry: &crate::GraphFileEntry,
    diagnostic: &Path,
) -> Result<u64, GfError> {
    let _phase = crate::lifecycle_io::PhaseScope::enter(crate::StorageIoPhase::ReadPathScan);
    checksum_handle(file, entry, diagnostic)
}

/// Open a workspace payload by name and admit it. For readers that must hold
/// a [`File`] rather than go through a [`crate::lifecycle_io::ReadPathFile`].
///
/// # Errors
/// Returns the open failure or the corruption refusal.
pub fn open_admitted(path: &Path) -> Result<File, GfError> {
    let file = File::open(path).map_err(|error| {
        GfError::Storage(format!("open graph payload {}: {error}", path.display()))
    })?;
    admit_file(&file)?;
    Ok(file)
}

/// One unadmitted hard-linked payload inode.
#[derive(Debug)]
struct PayloadTicket {
    entry: crate::GraphFileEntry,
    cas_object: PathBuf,
    workspace_root: PathBuf,
    workspace_file: PathBuf,
    batch: u64,
    outcome: OnceLock<Result<(), GfError>>,
    #[cfg(test)]
    checksum_runs: AtomicU64,
}

static REGISTRY: LazyLock<Mutex<HashMap<IdentityKey, Arc<PayloadTicket>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);

fn registry() -> MutexGuard<'static, HashMap<IdentityKey, Arc<PayloadTicket>>> {
    REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Tickets minted by one hydration share a batch, so a duplicate digest in the
/// same inventory (one inode, several logical paths) pays once, while a later
/// hydration of the same object mints a fresh ticket and re-admits it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdmissionBatch(u64);

impl AdmissionBatch {
    /// Start a hydration batch and drop tickets whose workspace is gone.
    pub(crate) fn begin() -> Self {
        let mut registry = registry();
        let mut live_roots = HashMap::<PathBuf, bool>::new();
        registry.retain(|_, ticket| {
            *live_roots
                .entry(ticket.workspace_root.clone())
                .or_insert_with_key(|root| root.exists())
        });
        Self(NEXT_BATCH.fetch_add(1, Ordering::Relaxed))
    }

    /// Require first-touch admission of `identity` against `entry`.
    pub(crate) fn register(
        self,
        identity: graphforge_filesystem::FileIdentity,
        entry: &crate::GraphFileEntry,
        cas_object: PathBuf,
        workspace_root: &Path,
        workspace_file: PathBuf,
    ) {
        let mut registry = registry();
        let key = key_of(identity);
        if registry
            .get(&key)
            .is_some_and(|existing| existing.batch == self.0)
        {
            return;
        }
        registry.insert(
            key,
            Arc::new(PayloadTicket {
                entry: entry.clone(),
                cas_object,
                workspace_root: workspace_root.to_path_buf(),
                workspace_file,
                batch: self.0,
                outcome: OnceLock::new(),
                #[cfg(test)]
                checksum_runs: AtomicU64::new(0),
            }),
        );
    }
}

impl PayloadTicket {
    /// A ticket describes `identity` only while some name still links it: the
    /// content-store object, or the workspace path it was hydrated at. A freed
    /// inode number reused by an unrelated file satisfies neither.
    fn describes(&self, identity: graphforge_filesystem::FileIdentity) -> bool {
        graphforge_filesystem::path_identity(&self.cas_object).ok() == Some(identity)
            || graphforge_filesystem::path_identity(&self.workspace_file).ok() == Some(identity)
    }

    fn admit(
        self: &Arc<Self>,
        file: &File,
        identity: graphforge_filesystem::FileIdentity,
    ) -> Result<(), GfError> {
        const MAX_TICKET_CHECKS: usize = 32;
        let mut ticket = Arc::clone(self);
        // Continuous hydration must not make one reader spin indefinitely.
        // Follow replacements without recursion, then fail closed on churn.
        for _ in 0..MAX_TICKET_CHECKS {
            // A newer hydration may have registered this same native identity
            // while an older reader still holds its ticket. Follow that ticket
            // before deciding whether this ticket still describes the inode.
            let replacement = {
                let registry = registry();
                match registry.get(&key_of(identity)) {
                    Some(current) if !Arc::ptr_eq(current, &ticket) => Some(Arc::clone(current)),
                    _ => None,
                }
            };
            if let Some(replacement) = replacement {
                ticket = replacement;
                continue;
            }
            // A native identity may be recycled after both authoritative names
            // disappear. Cached refusals belong only to the original payload.
            if !ticket.describes(identity) {
                let Some(replacement) = ticket.retire_and_get_replacement(identity) else {
                    return Ok(());
                };
                ticket = replacement;
                continue;
            }
            // Recheck after `describes`: a replacement could have been
            // registered while that path metadata was read. Both tickets can
            // still describe the same linked inode, so pointer identity is the
            // discriminator for whether the cached outcome is current.
            let (replacement, cached_outcome) = {
                let registry = registry();
                match registry.get(&key_of(identity)) {
                    Some(current) if !Arc::ptr_eq(current, &ticket) => {
                        (Some(Arc::clone(current)), None)
                    }
                    _ => (None, ticket.outcome.get().cloned()),
                }
            };
            if let Some(replacement) = replacement {
                ticket = replacement;
                continue;
            }
            if let Some(outcome) = cached_outcome {
                return outcome;
            }
            if let Some(outcome) = ticket.outcome.get() {
                return outcome.clone();
            }
            // No registry lock covers metadata or checksumming. Concurrent
            // first touches wait on this ticket's one computation; unrelated
            // inodes proceed. Replacements never nest checksum locks.
            let _phase = (!crate::lifecycle_io::phase_override_active()).then(|| {
                crate::lifecycle_io::PhaseScope::enter(crate::StorageIoPhase::ReadPathScan)
            });
            let outcome = ticket.outcome.get_or_init(|| {
                #[cfg(test)]
                ticket.checksum_runs.fetch_add(1, Ordering::Relaxed);
                checksum_handle(file, &ticket.entry, &ticket.cas_object).map(|_| ())
            });
            if outcome.is_ok() {
                let mut registry = registry();
                if registry
                    .get(&key_of(identity))
                    .is_some_and(|current| Arc::ptr_eq(current, &ticket))
                {
                    registry.remove(&key_of(identity));
                }
            }
            return outcome.clone();
        }
        Err(GfError::Validation(
            "graph payload admission authority changed too often before authentication".into(),
        ))
    }

    fn retire_and_get_replacement(
        self: &Arc<Self>,
        identity: graphforge_filesystem::FileIdentity,
    ) -> Option<Arc<Self>> {
        let mut registry = registry();
        let current = registry.get(&key_of(identity))?;
        if Arc::ptr_eq(current, self) {
            registry.remove(&key_of(identity));
            None
        } else {
            Some(Arc::clone(current))
        }
    }
}

/// Admit an opened payload handle. A handle that is not a registered
/// unadmitted inode is accepted unchanged.
///
/// # Errors
/// Returns the memoized length or checksum refusal for a corrupted payload.
pub fn admit_file(file: &File) -> Result<(), GfError> {
    if registry().is_empty() {
        return Ok(());
    }
    let identity = graphforge_filesystem::file_identity(file).map_err(|error| {
        GfError::Storage(format!("identify graph payload for admission: {error}"))
    })?;
    let ticket = registry().get(&key_of(identity)).cloned();
    match ticket {
        Some(ticket) => ticket.admit(file, identity),
        None => Ok(()),
    }
}

/// Admit the payload at `path` (following no link beyond the open) before a
/// reader that opens by name, such as a directory scan, touches it.
///
/// # Errors
/// Returns the open failure or the memoized corruption refusal.
pub fn admit_path(path: &Path) -> Result<(), GfError> {
    if registry().is_empty() {
        return Ok(());
    }
    let file = File::open(path).map_err(|error| {
        GfError::Storage(format!(
            "open graph payload {} for admission: {error}",
            path.display()
        ))
    })?;
    admit_file(&file)
}

/// Admit every regular file beneath `root`; used for directory-shaped readers.
///
/// # Errors
/// Returns the first traversal failure or corruption refusal.
pub fn admit_tree(root: &Path) -> Result<(), GfError> {
    if registry().is_empty() {
        return Ok(());
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory).map_err(|error| {
            GfError::Storage(format!(
                "read graph payload directory {}: {error}",
                directory.display()
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                GfError::Storage(format!(
                    "read graph payload directory {}: {error}",
                    directory.display()
                ))
            })?;
            let file_type = entry.file_type().map_err(|error| {
                GfError::Storage(format!(
                    "inspect graph payload {}: {error}",
                    entry.path().display()
                ))
            })?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                admit_path(&entry.path())?;
            }
        }
    }
    Ok(())
}

/// Number of payloads still waiting for first-touch admission. Diagnostic.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn pending_admissions() -> usize {
    registry().len()
}

struct ReadAt<'a> {
    file: &'a File,
    offset: u64,
}

impl std::io::Read for ReadAt<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(self.file, buffer, self.offset)?;
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(self.file, buffer, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }
}

/// Check one handle against its manifest entry by exact length and XXH64.
/// Positioned reads leave the caller's file offset untouched on Unix; on
/// Windows `seek_read` moves it, and no caller depends on the offset.
fn checksum_handle(
    file: &File,
    entry: &crate::GraphFileEntry,
    diagnostic: &Path,
) -> Result<u64, GfError> {
    let metadata = file.metadata().map_err(|error| {
        GfError::Storage(format!(
            "inspect graph payload {}: {error}",
            diagnostic.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(GfError::Validation(
            "graph payload length does not match checksum inventory".into(),
        ));
    }
    let mut reader = std::io::Read::take(
        ReadAt { file, offset: 0 },
        entry.byte_length.saturating_add(1),
    );
    let (actual, calls) = crate::graph_files::checksum_reader(&mut reader, diagnostic)?;
    if actual != entry.content_xxh64 {
        return Err(GfError::Validation(
            "graph payload XXH64 checksum does not match its inventory".into(),
        ));
    }
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        entry.byte_length,
        calls,
    );
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    Ok(calls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    struct Fixture {
        directory: tempfile::TempDir,
        object: PathBuf,
        link: PathBuf,
        entry: crate::GraphFileEntry,
    }

    impl Fixture {
        /// One content-store-like object and one hard link to it, as hydration
        /// leaves them.
        fn new(bytes: &[u8]) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let object = directory.path().join("object");
            let link = directory.path().join("workspace").join("topology.parquet");
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            std::fs::write(&object, bytes).unwrap();
            std::fs::hard_link(&object, &link).unwrap();
            let entry = crate::GraphFileEntry {
                relative_path: "topology.parquet".into(),
                byte_length: bytes.len() as u64,
                content_sha256: "0".repeat(64),
                content_xxh64: crate::corruption_checksum::checksum(bytes),
                role: crate::GraphFileRole::Topology,
            };
            Self {
                directory,
                object,
                link,
                entry,
            }
        }

        fn register(&self) {
            let identity = graphforge_filesystem::path_identity(&self.object).unwrap();
            AdmissionBatch::begin().register(
                identity,
                &self.entry,
                self.object.clone(),
                &self.directory.path().join("workspace"),
                self.link.clone(),
            );
        }

        /// Same-inode, same-length corruption of the shared object.
        fn flip_first_byte(&self) {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.object)
                .unwrap();
            let mut byte = [0_u8; 1];
            std::io::Read::read_exact(&mut file, &mut byte).unwrap();
            std::io::Seek::rewind(&mut file).unwrap();
            file.write_all(&[byte[0] ^ 0xff]).unwrap();
        }
    }

    const PAYLOAD: &[u8] = b"first touch authenticates this payload exactly once";

    #[test]
    fn a_clean_payload_is_admitted_through_any_of_its_links() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        admit_path(&fixture.link).unwrap();
        admit_path(&fixture.object).unwrap();
    }

    #[test]
    fn same_inode_same_length_corruption_is_refused_on_first_touch_via_either_name() {
        for through_link in [true, false] {
            let fixture = Fixture::new(PAYLOAD);
            fixture.register();
            fixture.flip_first_byte();
            assert_eq!(
                std::fs::metadata(&fixture.link).unwrap().len(),
                fixture.entry.byte_length,
                "the mutation must preserve length"
            );
            let path = if through_link {
                &fixture.link
            } else {
                &fixture.object
            };
            let error = admit_path(path).unwrap_err();
            assert!(error.to_string().contains("XXH64 checksum"), "{error}");
        }
    }

    #[test]
    fn a_refusal_is_memoized_and_never_downgraded() {
        for remove_object in [false, true] {
            let fixture = Fixture::new(PAYLOAD);
            fixture.register();
            fixture.flip_first_byte();
            let identity = graphforge_filesystem::path_identity(&fixture.object).unwrap();
            let ticket = registry().get(&key_of(identity)).cloned().unwrap();
            let first = admit_path(&fixture.link).unwrap_err();
            // Restoring the byte does not reopen the question for this hydration,
            // even when only one of the original authoritative names remains.
            fixture.flip_first_byte();
            let (removed, retained) = if remove_object {
                (&fixture.object, &fixture.link)
            } else {
                (&fixture.link, &fixture.object)
            };
            std::fs::remove_file(removed).unwrap();
            let second = admit_path(retained).unwrap_err();
            assert_eq!(first.to_string(), second.to_string());
            assert_eq!(ticket.checksum_runs.load(Ordering::Relaxed), 1);
        }
    }

    fn departed_refusal() -> (Fixture, Arc<PayloadTicket>) {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        fixture.flip_first_byte();
        let identity = graphforge_filesystem::path_identity(&fixture.object).unwrap();
        let ticket = registry().get(&key_of(identity)).cloned().unwrap();
        let error = admit_path(&fixture.link).unwrap_err();
        assert!(error.to_string().contains("XXH64 checksum"), "{error}");
        // Pin the old identity until its exact registry entry is removed, so
        // another parallel fixture cannot recycle it during this teardown.
        let retained = File::open(&fixture.object).unwrap();
        std::fs::remove_file(&fixture.object).unwrap();
        std::fs::remove_file(&fixture.link).unwrap();
        assert!(ticket.workspace_root.exists());
        assert!(Arc::ptr_eq(
            &registry().remove(&key_of(identity)).unwrap(),
            &ticket
        ));
        drop(retained);
        (fixture, ticket)
    }

    #[test]
    fn a_departed_cached_refusal_does_not_poison_a_reused_identity() {
        // Keep the old workspace directory alive so another hydration batch
        // cannot prune this ticket merely because its root disappeared.
        let (_departed, ticket) = departed_refusal();
        let unrelated = Fixture::new(b"an unrelated clean payload");
        unrelated.register();
        let file = File::open(&unrelated.object).unwrap();
        let identity = graphforge_filesystem::file_identity(&file).unwrap();
        let unrelated_ticket = registry().get(&key_of(identity)).cloned().unwrap();
        assert!(!ticket.describes(identity));
        // Model native identity recycling directly, without asking the OS to
        // recycle any particular inode number or clearing other tickets.
        assert!(Arc::ptr_eq(
            &registry()
                .insert(key_of(identity), Arc::clone(&ticket))
                .unwrap(),
            &unrelated_ticket
        ));
        admit_file(&file).expect("a departed refusal must not apply to unrelated bytes");
        assert!(!registry().contains_key(&key_of(identity)));
        assert_eq!(ticket.checksum_runs.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retiring_a_departed_refusal_preserves_a_replacement_ticket() {
        let (_departed, stale) = departed_refusal();
        let unrelated = Fixture::new(b"a newly admitted payload");
        unrelated.register();
        let file = File::open(&unrelated.object).unwrap();
        let identity = graphforge_filesystem::file_identity(&file).unwrap();
        let replacement = registry().get(&key_of(identity)).cloned().unwrap();
        assert!(!stale.describes(identity));
        unrelated.flip_first_byte();
        // An in-flight reader may still hold the retired Arc after a newer
        // hydration has replaced its entry at this identity key. Its very next
        // access must authenticate the replacement before returning any bytes.
        let error = stale.admit(&file, identity).unwrap_err();
        assert!(error.to_string().contains("XXH64 checksum"), "{error}");
        assert!(Arc::ptr_eq(
            registry().get(&key_of(identity)).unwrap(),
            &replacement
        ));
        let memoized = admit_file(&file).unwrap_err();
        assert_eq!(error.to_string(), memoized.to_string());
        assert_eq!(replacement.checksum_runs.load(Ordering::Relaxed), 1);
        assert_eq!(stale.checksum_runs.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_stale_cached_success_authenticates_a_same_identity_replacement() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        let identity = graphforge_filesystem::path_identity(&fixture.object).unwrap();
        let stale = registry().get(&key_of(identity)).cloned().unwrap();
        let file = File::open(&fixture.link).unwrap();
        stale.admit(&file, identity).unwrap();
        assert!(stale.outcome.get().unwrap().is_ok());

        fixture.register();
        let replacement = registry().get(&key_of(identity)).cloned().unwrap();
        assert!(!Arc::ptr_eq(&stale, &replacement));
        assert!(stale.describes(identity));
        fixture.flip_first_byte();

        let error = stale.admit(&file, identity).unwrap_err();
        assert!(error.to_string().contains("XXH64 checksum"), "{error}");
        assert_eq!(stale.checksum_runs.load(Ordering::Relaxed), 1);
        assert_eq!(replacement.checksum_runs.load(Ordering::Relaxed), 1);
        assert!(Arc::ptr_eq(
            registry().get(&key_of(identity)).unwrap(),
            &replacement
        ));
    }

    #[test]
    fn stale_and_current_readers_share_replacement_admission() {
        let (_departed, stale) = departed_refusal();
        let unrelated = Fixture::new(&vec![7_u8; 4 << 20]);
        unrelated.register();
        let file = File::open(&unrelated.object).unwrap();
        let identity = graphforge_filesystem::file_identity(&file).unwrap();
        let replacement = registry().get(&key_of(identity)).cloned().unwrap();
        std::thread::scope(|scope| {
            for reader in 0..8 {
                let stale = &stale;
                let file = &file;
                scope.spawn(move || {
                    if reader % 2 == 0 {
                        stale.admit(file, identity).unwrap();
                    } else {
                        admit_file(file).unwrap();
                    }
                });
            }
        });
        assert_eq!(replacement.checksum_runs.load(Ordering::Relaxed), 1);
        assert_eq!(stale.checksum_runs.load(Ordering::Relaxed), 1);
        assert!(!registry().contains_key(&key_of(identity)));
    }

    #[test]
    fn a_resized_payload_is_refused_by_length() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fixture.object)
            .unwrap()
            .set_len(PAYLOAD.len() as u64 - 1)
            .unwrap();
        let error = admit_path(&fixture.link).unwrap_err();
        assert!(error.to_string().contains("length"), "{error}");
    }

    #[test]
    fn a_later_hydration_of_the_same_object_admits_it_again() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        admit_path(&fixture.link).unwrap();
        fixture.flip_first_byte();
        // A new open mints a new ticket, so the corruption is found.
        fixture.register();
        assert!(admit_path(&fixture.link).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn admission_does_not_move_the_callers_file_offset() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.register();
        let mut file = File::open(&fixture.link).unwrap();
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(7)).unwrap();
        admit_file(&file).unwrap();
        let mut rest = Vec::new();
        std::io::Read::read_to_end(&mut file, &mut rest).unwrap();
        assert_eq!(rest, &PAYLOAD[7..]);
    }

    #[test]
    fn files_that_were_never_registered_pass_through() {
        let fixture = Fixture::new(PAYLOAD);
        fixture.flip_first_byte();
        admit_path(&fixture.link).unwrap();
    }

    #[test]
    fn a_ticket_for_a_freed_inode_does_not_apply_to_an_unrelated_file() {
        // Neither the content-store name nor the workspace name still links
        // the ticket's inode, as after a reused inode number.
        let unrelated = Fixture::new(b"an unrelated payload");
        let stale = PayloadTicketProbe::pointing_nowhere(&unrelated.entry);
        stale.register_for(&unrelated.object);
        unrelated.flip_first_byte();
        admit_path(&unrelated.object).unwrap();
    }

    struct PayloadTicketProbe {
        entry: crate::GraphFileEntry,
    }

    impl PayloadTicketProbe {
        fn pointing_nowhere(entry: &crate::GraphFileEntry) -> Self {
            Self {
                entry: entry.clone(),
            }
        }

        fn register_for(&self, path: &Path) {
            let identity = graphforge_filesystem::path_identity(path).unwrap();
            let gone = path.with_file_name("freed");
            let root = gone.parent().unwrap().to_path_buf();
            AdmissionBatch::begin().register(identity, &self.entry, gone.clone(), &root, gone);
        }
    }

    #[test]
    fn concurrent_first_touches_share_one_checksum() {
        let fixture = Fixture::new(&vec![7_u8; 4 << 20]);
        fixture.register();
        let identity = graphforge_filesystem::path_identity(&fixture.link).unwrap();
        let ticket = registry().get(&key_of(identity)).cloned().unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| admit_path(&fixture.link).unwrap());
            }
        });
        assert_eq!(
            ticket.checksum_runs.load(Ordering::Relaxed),
            1,
            "eight concurrent first touches of one inode must checksum it once"
        );
        assert!(registry().get(&key_of(identity)).is_none());
    }
}
