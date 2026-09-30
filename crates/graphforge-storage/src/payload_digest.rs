//! SHA-256 used to name or fully verify payload bytes. Test accounting excludes
//! control-object hashing and adds no counters to production operations.

use sha2::Digest;

pub(crate) struct PayloadSha256(sha2::Sha256);

impl PayloadSha256 {
    pub(crate) fn new() -> Self {
        Self(sha2::Sha256::new())
    }

    pub(crate) fn update(&mut self, bytes: impl AsRef<[u8]>) {
        let bytes = bytes.as_ref();
        #[cfg(test)]
        HASHED_BYTES.with(|count| {
            count.set(
                count
                    .get()
                    .checked_add(bytes.len() as u64)
                    .expect("test payload hash byte count overflow"),
            )
        });
        self.0.update(bytes);
    }

    pub(crate) fn finalize(self) -> sha2::digest::Output<sha2::Sha256> {
        self.0.finalize()
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
