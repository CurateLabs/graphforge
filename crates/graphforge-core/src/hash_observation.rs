//! SHA-256 with byte and elapsed-time observation during explicit captures.
use sha2::digest::{FixedOutput, HashMarker, Output, OutputSizeUser, Reset, Update};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
static CAPTURES: AtomicUsize = AtomicUsize::new(0);

/// Activates process-wide SHA-256 observation without resetting other captures.
#[derive(Debug)]
pub struct HashObservation(());
impl HashObservation {
    /// Begin optional observation on all process threads.
    #[must_use]
    pub fn start() -> Self {
        CAPTURES.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}
impl Drop for HashObservation {
    fn drop(&mut self) {
        CAPTURES.fetch_sub(1, Ordering::Relaxed);
    }
}
fn active() -> bool {
    CAPTURES.load(Ordering::Relaxed) != 0
}
use std::time::Instant;

static BYTES: AtomicU64 = AtomicU64::new(0);
static NANOS: AtomicU64 = AtomicU64::new(0);

/// SHA-256, preserving digest bytes while observing work on every process thread.
#[derive(Clone)]
pub struct ObservedSha256(
    sha2::Sha256,
    #[cfg(any(test, feature = "test-support"))] HashDomain,
);

/// Why cryptographic input bytes are consumed. Payload identity is never control metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashDomain {
    /// Immutable durable artifact bytes, including CAS identity and full verification.
    ArtifactPayload,
    /// Canonical identity required by a public contract.
    ContractIdentity,
    /// Authenticated control objects and receipts.
    ControlAuthentication,
    /// Optional diagnostic evidence.
    OptionalEvidence,
    /// A producer that has not supplied a domain.
    Unclassified,
}

impl Default for ObservedSha256 {
    fn default() -> Self {
        Self::for_domain(HashDomain::Unclassified)
    }
}

impl ObservedSha256 {
    /// Construct a SHA-256 producer with its actual input domain.
    #[must_use]
    pub fn for_domain(domain: HashDomain) -> Self {
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = domain;
        Self(
            sha2::Sha256::default(),
            #[cfg(any(test, feature = "test-support"))]
            domain,
        )
    }
}

/// Domain-specific cryptographic producers retain the standard `Digest` API.
#[derive(Clone)]
pub struct DomainSha256<const DOMAIN: u8>(ObservedSha256);
/// Cryptographic durable artifact identity and full verification.
pub type ArtifactSha256 = DomainSha256<0>;
/// Canonical contract identities.
pub type ContractSha256 = DomainSha256<1>;
/// Control object and receipt authentication.
pub type ControlSha256 = DomainSha256<2>;
/// Optional evidence identity.
pub type EvidenceSha256 = DomainSha256<3>;
impl<const DOMAIN: u8> Default for DomainSha256<DOMAIN> {
    fn default() -> Self {
        let domain = match DOMAIN {
            0 => HashDomain::ArtifactPayload,
            1 => HashDomain::ContractIdentity,
            2 => HashDomain::ControlAuthentication,
            3 => HashDomain::OptionalEvidence,
            _ => HashDomain::Unclassified,
        };
        Self(ObservedSha256::for_domain(domain))
    }
}
impl<const DOMAIN: u8> OutputSizeUser for DomainSha256<DOMAIN> {
    type OutputSize = <sha2::Sha256 as OutputSizeUser>::OutputSize;
}
impl<const DOMAIN: u8> HashMarker for DomainSha256<DOMAIN> {}
impl<const DOMAIN: u8> Update for DomainSha256<DOMAIN> {
    fn update(&mut self, data: &[u8]) {
        Update::update(&mut self.0, data);
    }
}
impl<const DOMAIN: u8> FixedOutput for DomainSha256<DOMAIN> {
    fn finalize_into(self, out: &mut Output<Self>) {
        FixedOutput::finalize_into(self.0, out);
    }
}
impl<const DOMAIN: u8> Reset for DomainSha256<DOMAIN> {
    fn reset(&mut self) {
        Reset::reset(&mut self.0);
    }
}
impl OutputSizeUser for ObservedSha256 {
    type OutputSize = <sha2::Sha256 as OutputSizeUser>::OutputSize;
}
impl HashMarker for ObservedSha256 {}
impl Update for ObservedSha256 {
    fn update(&mut self, data: &[u8]) {
        #[cfg(any(test, feature = "test-support"))]
        operation::record_hash(self.1, data.len() as u64);
        if !active() {
            Update::update(&mut self.0, data);
            return;
        }
        let started = Instant::now();
        Update::update(&mut self.0, data);
        NANOS.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        BYTES.fetch_add(data.len() as u64, Ordering::Relaxed);
    }
}
impl FixedOutput for ObservedSha256 {
    fn finalize_into(self, out: &mut Output<Self>) {
        if !active() {
            FixedOutput::finalize_into(self.0, out);
            return;
        }
        let started = Instant::now();
        FixedOutput::finalize_into(self.0, out);
        NANOS.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}
impl Reset for ObservedSha256 {
    fn reset(&mut self) {
        Reset::reset(&mut self.0);
    }
}
/// Cumulative input bytes and update/finalize elapsed nanoseconds.
#[must_use]
pub fn totals() -> (u64, u64) {
    (BYTES.load(Ordering::Relaxed), NANOS.load(Ordering::Relaxed))
}

/// Transfer the current operation to an owned worker. In production this is empty.
#[derive(Clone, Default)]
pub struct OperationContext(#[cfg(any(test, feature = "test-support"))] operation::Context);
impl OperationContext {
    #[must_use]
    /// Capture the initiating operation without resetting counters.
    pub fn capture() -> Self {
        Self(
            #[cfg(any(test, feature = "test-support"))]
            operation::Context::capture(),
        )
    }
    #[must_use]
    /// Attach this operation until the returned guard drops.
    pub fn attach(&self) -> OperationGuard {
        OperationGuard {
            #[cfg(any(test, feature = "test-support"))]
            _guard: self.0.attach(),
        }
    }
}
/// Restores the initiating thread's previous operation after worker completion.
pub struct OperationGuard {
    #[cfg(any(test, feature = "test-support"))]
    _guard: operation::Guard,
}

/// Account an actual admitted topology projection only when test support is compiled in.
#[inline(always)]
pub fn record_topology_projection() {
    #[cfg(any(test, feature = "test-support"))]
    operation::record_topology_projection();
}

/// Isolated operation accounting used by Rust facade tests. Captures neither reset nor
/// sample process-wide counters; worker handles attach only to their own operation.
#[cfg(any(test, feature = "test-support"))]
pub mod operation {
    use super::HashDomain;
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct Counters {
        hash: [AtomicU64; 5],
        checksum: AtomicU64,
        topology_projections: AtomicU64,
    }
    thread_local! {
        static CURRENT: RefCell<Option<Arc<Counters>>> = const { RefCell::new(None) };
    }
    /// Exact application input bytes submitted to actual producers in one operation.
    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    pub struct Snapshot {
        /// Durable artifact input bytes consumed by actual cryptographic producers.
        pub artifact_payload_sha256_bytes: u64,
        /// Canonical public-contract identity preimage bytes.
        pub contract_identity_sha256_bytes: u64,
        /// Required control object and receipt authentication input bytes.
        pub control_authentication_sha256_bytes: u64,
        /// Optional evidence identity input bytes.
        pub optional_evidence_sha256_bytes: u64,
        /// Unclassified cryptographic input bytes; admission tests require zero.
        pub unclassified_sha256_bytes: u64,
        /// Exact bytes submitted to actual corruption-checksum producers.
        pub checksum_bytes: u64,
        /// Actual admitted label topology projections started by this operation.
        pub topology_projections: u64,
    }
    /// Captured operation context, transferable to synchronous worker threads.
    #[derive(Clone, Default)]
    pub struct Context(Option<Arc<Counters>>);
    impl Context {
        #[must_use]
        /// Capture the initiating operation without resetting counters.
        pub fn capture() -> Self {
            CURRENT.with(|current| Self(current.borrow().clone()))
        }
        #[must_use]
        /// Attach this operation until the returned guard drops.
        pub fn attach(&self) -> Guard {
            let previous = CURRENT.with(|current| current.replace(self.0.clone()));
            Guard {
                previous,
                _thread: std::marker::PhantomData,
            }
        }
    }
    /// Restores the previous operation on drop, including nested and panicking workers.
    pub struct Guard {
        previous: Option<Arc<Counters>>,
        _thread: std::marker::PhantomData<std::rc::Rc<()>>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CURRENT.with(|current| {
                current.replace(self.previous.take());
            });
        }
    }
    /// Owns one operation's counters and attaches the initiating thread.
    pub struct Capture {
        counters: Arc<Counters>,
        _guard: Guard,
    }
    impl Capture {
        #[must_use]
        /// Begin an isolated capture on the initiating thread.
        pub fn start() -> Self {
            let counters = Arc::new(Counters::default());
            let context = Context(Some(Arc::clone(&counters)));
            Self {
                counters,
                _guard: context.attach(),
            }
        }
        #[must_use]
        /// Transfer this capture to an owned worker.
        pub fn context(&self) -> Context {
            Context(Some(Arc::clone(&self.counters)))
        }
        #[must_use]
        /// Read cumulative actual producer work for this operation.
        pub fn snapshot(&self) -> Snapshot {
            let load = |index: usize| self.counters.hash[index].load(Ordering::Relaxed);
            Snapshot {
                artifact_payload_sha256_bytes: load(0),
                contract_identity_sha256_bytes: load(1),
                control_authentication_sha256_bytes: load(2),
                optional_evidence_sha256_bytes: load(3),
                unclassified_sha256_bytes: load(4),
                checksum_bytes: self.counters.checksum.load(Ordering::Relaxed),
                topology_projections: self.counters.topology_projections.load(Ordering::Relaxed),
            }
        }
    }
    pub(super) fn record_hash(domain: HashDomain, bytes: u64) {
        let index = match domain {
            HashDomain::ArtifactPayload => 0,
            HashDomain::ContractIdentity => 1,
            HashDomain::ControlAuthentication => 2,
            HashDomain::OptionalEvidence => 3,
            HashDomain::Unclassified => 4,
        };
        CURRENT.with(|current| {
            if let Some(counters) = current.borrow().as_ref() {
                counters.hash[index].fetch_add(bytes, Ordering::Relaxed);
            }
        });
    }
    /// Called by the actual streaming corruption-checksum producer.
    pub fn record_checksum(bytes: u64) {
        CURRENT.with(|current| {
            if let Some(counters) = current.borrow().as_ref() {
                counters.checksum.fetch_add(bytes, Ordering::Relaxed);
            }
        });
    }
    pub(super) fn record_topology_projection() {
        CURRENT.with(|current| {
            if let Some(counters) = current.borrow().as_ref() {
                counters
                    .topology_projections
                    .fetch_add(1, Ordering::Relaxed);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{HashDomain, ObservedSha256, operation::Capture};
    use sha2::Digest;

    #[test]
    fn operation_captures_isolate_parallel_workers_and_restore_nested_scopes() {
        let outer = Capture::start();
        let first_context = outer.context();
        let second = std::thread::spawn(|| {
            let capture = Capture::start();
            let context = capture.context();
            std::thread::spawn(move || {
                let _attached = context.attach();
                let mut hash = ObservedSha256::for_domain(HashDomain::ArtifactPayload);
                hash.update([0_u8; 29]);
                hash.finalize();
            })
            .join()
            .unwrap();
            capture.snapshot()
        });
        std::thread::spawn(move || {
            let _attached = first_context.attach();
            let mut hash = ObservedSha256::for_domain(HashDomain::ArtifactPayload);
            hash.update([0_u8; 17]);
            hash.finalize();
            {
                let nested = Capture::start();
                let mut hash = ObservedSha256::for_domain(HashDomain::ControlAuthentication);
                hash.update([0_u8; 11]);
                hash.finalize();
                assert_eq!(nested.snapshot().control_authentication_sha256_bytes, 11);
            }
            super::operation::record_checksum(7);
        })
        .join()
        .unwrap();
        assert_eq!(outer.snapshot().artifact_payload_sha256_bytes, 17);
        assert_eq!(outer.snapshot().control_authentication_sha256_bytes, 0);
        assert_eq!(outer.snapshot().checksum_bytes, 7);
        assert_eq!(second.join().unwrap().artifact_payload_sha256_bytes, 29);
    }
}
