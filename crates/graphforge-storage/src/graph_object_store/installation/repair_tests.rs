//! Explicit repair of a corrupt sealed CAS object (#1738): authenticated
//! replacement, retained readers, allocation, and uncertain native outcomes.
use super::*;
use crate::durable_commit::{fault, observe_barriers};
use crate::graph_object_store::{Sha256, verify_graph_object};
use graphforge_filesystem::{FileIdentity, file_identity, file_space_usage};

const PAYLOAD_BYTES: usize = 24 * 1024;

struct Repair {
    root: tempfile::TempDir,
    source: std::path::PathBuf,
    lease: GraphObjectPublicationLease,
    operation: crate::StorageAllocationOperation,
    digest: String,
    payload: Vec<u8>,
    corrupt: Vec<u8>,
    object: std::path::PathBuf,
    prior: FileIdentity,
}

impl Repair {
    /// A corrupt object at the address of `payload`, sealed by the production
    /// CAS writer of this platform, with its allocation already recorded.
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let payload = (0..PAYLOAD_BYTES)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        let mut corrupt = payload.clone();
        corrupt[PAYLOAD_BYTES / 2] ^= 1;
        let digest = hex_digest(Sha256::digest(&payload).into());
        let source = root.path().join("rebuilt.csr");
        fs::write(&source, &payload).unwrap();
        let operation = crate::StorageAllocationOperation::default();
        let mut lease = begin_graph_object_publication(root.path()).unwrap();
        lease.set_allocation_operation(Some(operation.clone()));
        let prior = plant_sealed_object(&lease, &digest, &corrupt);
        let object = graph_object_path(root.path(), &digest).unwrap();
        operation
            .replace_file_at(&object, &File::open(&object).unwrap())
            .unwrap();
        Self {
            root,
            source,
            lease,
            operation,
            digest,
            payload,
            corrupt,
            object,
            prior,
        }
    }

    fn repair(&self) -> Result<GraphObjectInstallEvidence, GfError> {
        install_graph_object_file_repairing_with_lease(
            &self.lease,
            &self.source,
            &self.digest,
            self.payload.len() as u64,
        )
    }

    fn visible_identity(&self) -> FileIdentity {
        graphforge_filesystem::path_identity(&self.object).unwrap()
    }

    fn temporary_names(&self) -> usize {
        fs::read_dir(self.root.path().join(GRAPH_OBJECTS_DIR).join(TEMP_DIR))
            .unwrap()
            .count()
    }

    fn bucket_names(&self) -> usize {
        fs::read_dir(self.object.parent().unwrap()).unwrap().count()
    }

    /// The operation charges exactly the object now at the address.
    fn assert_allocation_is_the_visible_object(&self) {
        let visible = File::open(&self.object).unwrap();
        assert_eq!(
            self.operation.totals().unwrap().0,
            file_space_usage(&visible).unwrap().allocated_bytes
        );
    }

    /// The object at the address is the authenticated payload, sealed.
    fn assert_repaired(&self) {
        assert_ne!(self.visible_identity(), self.prior);
        assert_eq!(fs::read(&self.object).unwrap(), self.payload);
        verify_graph_object(self.root.path(), &self.digest, self.payload.len() as u64).unwrap();
        assert!(fs::metadata(&self.object).unwrap().permissions().readonly());
        #[cfg(windows)]
        {
            // The canonical sealed reader admits only the read-only attribute
            // together with the protected CAS DACL.
            let bucket = self.lease.cas.digest_bucket(&self.digest, false).unwrap();
            bucket
                .open_cas_child_file(std::ffi::OsStr::new(&self.digest[2..]))
                .unwrap();
        }
        assert_eq!(self.temporary_names(), 0);
        assert_eq!(self.bucket_names(), 1);
    }

    /// Nothing at the address changed.
    fn assert_address_unchanged(&self) {
        assert_eq!(self.visible_identity(), self.prior);
        assert_eq!(fs::read(&self.object).unwrap(), self.corrupt);
        assert_eq!(self.bucket_names(), 1);
    }

    /// Nothing at the address changed and the sealed staged copy was retired.
    fn assert_unrepaired(&self) {
        self.assert_address_unchanged();
        assert_eq!(self.temporary_names(), 0);
    }
}

fn plant_sealed_object(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    bytes: &[u8],
) -> FileIdentity {
    let bucket = lease.cas.digest_bucket(digest, true).unwrap();
    let name = std::ffi::OsStr::new(&digest[2..]);
    #[cfg(unix)]
    {
        let mut file = bucket.create_child_file(name).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        let mut permissions = file.metadata().unwrap().permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions).unwrap();
        file_identity(&file).unwrap()
    }
    #[cfg(windows)]
    {
        let mut writer = bucket.create_cas_child_file(name).unwrap();
        writer.write_all(bytes).unwrap();
        writer.sync_all().unwrap();
        let sealed = bucket
            .seal_cas_child_file(name, writer)
            .unwrap()
            .into_file();
        file_identity(&sealed).unwrap()
    }
}

fn read_retained(mut file: &File) -> Vec<u8> {
    file.rewind().unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    bytes
}

/// The replacement is a new inode at the address. A reader that opened the
/// corrupt object first keeps reading those exact bytes; nothing writes the
/// old inode.
#[test]
fn sealed_cas_repair_replaces_the_object_and_preserves_an_open_old_reader() {
    let repair = Repair::new();
    // An ordinary reader shares deletion, so replacement can proceed on every
    // platform while it stays open.
    let reader = File::open(&repair.object).unwrap();
    assert!(
        verify_graph_object(
            repair.root.path(),
            &repair.digest,
            repair.payload.len() as u64
        )
        .is_err()
    );
    let evidence = repair.repair().unwrap();
    assert!(evidence.attempted_install && !evidence.reused_existing);
    assert_eq!(evidence.bytes_installed, repair.payload.len() as u64);
    repair.assert_repaired();
    assert_eq!(file_identity(&reader).unwrap(), repair.prior);
    assert_eq!(read_retained(&reader), repair.corrupt);
    repair.assert_allocation_is_the_visible_object();
    // The repaired object now authenticates, so a second repair reuses it.
    let reused = repair.repair().unwrap();
    assert!(reused.reused_existing);
}

/// The source of a repair must hash to the address it replaces.
#[test]
fn sealed_cas_repair_refuses_a_source_that_is_not_the_address() {
    let repair = Repair::new();
    fs::write(&repair.source, &repair.corrupt).unwrap();
    let error = repair.repair().unwrap_err();
    assert!(
        error.to_string().contains("digest or length changed"),
        "{error}"
    );
    // The refused copy is never sealed; like any failed install it leaves
    // only unreferenced temporary residue, never a change at the address.
    repair.assert_address_unchanged();
}

/// A native replacement that took effect but reported an unknown outcome
/// keeps its original failure, and is acknowledged exactly like a certain
/// replacement: the same allocation and the same namespace fences.
#[test]
fn uncertain_repair_visibility_preserves_failure_and_reconciles_allocation() {
    let certain = Repair::new();
    let (result, certain_fences) = observe_barriers(|| certain.repair());
    result.unwrap();
    certain.assert_repaired();
    certain.assert_allocation_is_the_visible_object();

    let repair = Repair::new();
    let reader = File::open(&repair.object).unwrap();
    fault::arm(fault::Point::NativeUnknown);
    let (result, fences) = observe_barriers(|| repair.repair());
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("durable commit injected NativeUnknown"),
        "{error}"
    );
    repair.assert_repaired();
    assert_eq!(read_retained(&reader), repair.corrupt);
    repair.assert_allocation_is_the_visible_object();
    assert_eq!(
        fences, certain_fences,
        "the visible replacement's namespaces are acknowledged"
    );
}

#[test]
fn failed_repair_before_visibility_preserves_prior_allocation() {
    let repair = Repair::new();
    fault::arm(fault::Point::BeforeVisible);
    let error = repair.repair().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("durable commit injected BeforeVisible"),
        "{error}"
    );
    repair.assert_unrepaired();
    repair.assert_allocation_is_the_visible_object();
}

#[test]
fn failed_repair_parent_fence_preserves_visible_allocation() {
    let repair = Repair::new();
    fault::arm(fault::Point::ParentFence);
    let error = repair.repair().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("durable commit injected ParentFence"),
        "{error}"
    );
    repair.assert_repaired();
    repair.assert_allocation_is_the_visible_object();
}

/// Repair authority never extends to an object that is merely unreadable or
/// of the wrong length: only a sealed digest mismatch is replaceable.
#[test]
fn sealed_cas_repair_refuses_an_object_of_another_length() {
    let repair = Repair::new();
    let bucket = repair
        .lease
        .cas
        .digest_bucket(&repair.digest, false)
        .unwrap();
    let name = std::ffi::OsStr::new(&repair.digest[2..]);
    bucket.unlink_child_if_identity(name, repair.prior).unwrap();
    let short = plant_sealed_object(&repair.lease, &repair.digest, &repair.corrupt[1..]);
    let error = repair.repair().unwrap_err();
    assert!(!error.to_string().contains("injected"), "{error}");
    assert_eq!(repair.visible_identity(), short);
    assert_eq!(
        fs::read(&repair.object).unwrap(),
        &repair.corrupt[1..],
        "{error}"
    );
}
