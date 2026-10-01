//! SHA-256 used to name or fully verify payload bytes. Test accounting excludes
//! control-object hashing. Explicit region captures observe payload hashing too.

use crate::concurrency_attribution::ObservedSha256 as Sha256;
use sha2::Digest;

#[cfg(any(test, feature = "test-support"))]
pub use graphforge_core::hash_observation::operation::{
    Capture as PayloadDigestCapture, Context as PayloadDigestContext,
    Snapshot as PayloadDigestSnapshot,
};

pub(crate) struct PayloadSha256(Sha256, #[cfg(test)] bool);

impl PayloadSha256 {
    pub(crate) fn new() -> Self {
        Self::for_domain(graphforge_core::hash_observation::HashDomain::ArtifactPayload)
    }

    /// Stream file payload bytes into an identity of the caller's domain. Only
    /// artifact-payload identities count toward the payload hashing total.
    pub(crate) fn for_domain(domain: graphforge_core::hash_observation::HashDomain) -> Self {
        Self(
            Sha256::for_domain(domain),
            #[cfg(test)]
            {
                domain == graphforge_core::hash_observation::HashDomain::ArtifactPayload
            },
        )
    }

    pub(crate) fn update(&mut self, bytes: impl AsRef<[u8]>) {
        let bytes = bytes.as_ref();
        #[cfg(test)]
        if self.1 {
            HASHED_BYTES.with(|count| {
                count.set(
                    count
                        .get()
                        .checked_add(bytes.len() as u64)
                        .expect("test payload hash byte count overflow"),
                )
            });
        }
        self.0.update(bytes);
    }

    pub(crate) fn finalize(self) -> sha2::digest::Output<sha2::Sha256> {
        self.0.finalize()
    }

    pub(crate) fn digest(bytes: impl AsRef<[u8]>) -> sha2::digest::Output<sha2::Sha256> {
        let mut hasher = Self::new();
        hasher.update(bytes);
        hasher.finalize()
    }
}

#[cfg(test)]
thread_local! {
    static HASHED_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_hashed_bytes() -> u64 {
    HASHED_BYTES.with(|count| count.replace(0))
}
