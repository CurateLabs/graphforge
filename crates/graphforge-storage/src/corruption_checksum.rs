//! Non-cryptographic payload checksum for bytes this process already named.
//!
//! GraphForge uses two hash roles and they are deliberately not the same
//! primitive (#1384, accepted decision 2 and 3):
//!
//! * **Naming.** A digest that *names* an object — a content-addressed graph
//!   object, an artifact receipt's `sha256`, any authority digest — is SHA-256
//!   and stays SHA-256. `same name means same bytes` is a correctness
//!   requirement for identity and deduplication there, so collision resistance
//!   is not negotiable and this module must never be used for it.
//! * **Corruption detection.** A checksum that only answers *did these bytes
//!   change since this system wrote them* does not need collision resistance.
//!   That is what [`Checksum`] is for, and it is what
//!   `authenticate_shaped_output` uses to refuse a same-inode, same-length
//!   payload mutation of a completed shape output (#1392, same class as #1269).
//!
//! # The two assumptions this rests on
//!
//! Recorded here, beside the code, and not only in the issue. **If either one
//! stops holding, the decision reverts and this checksum must be replaced by a
//! cryptographic digest.**
//!
//! 1. **The threat model excludes an active same-identity adversary.** ADR 0013
//!    states that the operational boundary does not claim protection against an
//!    actively malicious same-identity process. A non-cryptographic checksum is
//!    trivially forgeable, so it defends against accidental byte mutation, a
//!    torn or partial write, and silent media corruption — not against an
//!    attacker who can rewrite the receipt alongside the payload. If GraphForge
//!    is ever deployed onto untrusted shared storage, a multi-tenant host, or
//!    under supply-chain threat, revisit this.
//! 2. **The storage substrate does not checksum file data.** The reference
//!    deployment is ext4 on an mdadm RAID 1 pair. ext4's `metadata_csum` covers
//!    inodes, directory blocks and the journal with CRC32c; it does not cover
//!    file data, and RAID 1 mirrors blocks without a checksum, so it cannot
//!    tell which of two disagreeing copies is correct. That is why GraphForge
//!    verifies once rather than never. On a substrate that does checksum file
//!    data (ZFS, btrfs) this check could be relaxed further; on a weaker one it
//!    may need strengthening.
//!
//! # The primitive
//!
//! XXH64, seed 0, as specified by the xxHash project and implemented here so
//! the storage crate takes no new dependency for it. It is ~4-5x faster than
//! hardware-accelerated SHA-256 on this host and detects the byte mutations
//! above with probability `1 - 2^-64` per payload. The value is recorded in
//! `ArtifactReceipt::xxh64` beside `sha256`, computed by the same single pass
//! that writes the bytes — no pass reads a payload back in order to checksum
//! it.

// Constants are written in the same form as the reference C implementation.
const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

const STRIPE_BYTES: usize = 32;

/// Streaming XXH64 checksum over a payload, seed 0.
///
/// Feed bytes with [`Checksum::update`] in any chunking; [`Checksum::finish`]
/// is a pure function of the concatenated byte sequence.
#[derive(Clone, Debug)]
pub(crate) struct Checksum {
    accumulators: [u64; 4],
    buffer: [u8; STRIPE_BYTES],
    buffered: usize,
    length: u64,
}

impl Default for Checksum {
    fn default() -> Self {
        Self::new()
    }
}

impl Checksum {
    /// A checksum over the empty payload.
    pub(crate) const fn new() -> Self {
        Self {
            accumulators: [
                PRIME64_1.wrapping_add(PRIME64_2),
                PRIME64_2,
                0,
                0_u64.wrapping_sub(PRIME64_1),
            ],
            buffer: [0; STRIPE_BYTES],
            buffered: 0,
            length: 0,
        }
    }

    /// Append `bytes` to the checksummed payload.
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.length = self.length.wrapping_add(bytes.len() as u64);
        let mut rest = bytes;
        if self.buffered != 0 {
            let wanted = STRIPE_BYTES - self.buffered;
            let taken = wanted.min(rest.len());
            self.buffer[self.buffered..self.buffered + taken].copy_from_slice(&rest[..taken]);
            self.buffered += taken;
            rest = &rest[taken..];
            if self.buffered < STRIPE_BYTES {
                return;
            }
            let stripe = self.buffer;
            self.absorb(&stripe);
            self.buffered = 0;
        }
        while let Some((stripe, tail)) = rest.split_first_chunk::<STRIPE_BYTES>() {
            self.absorb(stripe);
            rest = tail;
        }
        self.buffer[..rest.len()].copy_from_slice(rest);
        self.buffered = rest.len();
    }

    fn absorb(&mut self, stripe: &[u8; STRIPE_BYTES]) {
        for (index, accumulator) in self.accumulators.iter_mut().enumerate() {
            let mut lane = [0_u8; 8];
            lane.copy_from_slice(&stripe[index * 8..index * 8 + 8]);
            *accumulator = round(*accumulator, u64::from_le_bytes(lane));
        }
    }

    /// The checksum of everything fed so far. Does not consume the state.
    pub(crate) fn finish(&self) -> u64 {
        let mut accumulator = if self.length < STRIPE_BYTES as u64 {
            PRIME64_5
        } else {
            let [one, two, three, four] = self.accumulators;
            let mut converged = one
                .rotate_left(1)
                .wrapping_add(two.rotate_left(7))
                .wrapping_add(three.rotate_left(12))
                .wrapping_add(four.rotate_left(18));
            for lane in [one, two, three, four] {
                converged ^= round(0, lane);
                converged = converged.wrapping_mul(PRIME64_1).wrapping_add(PRIME64_4);
            }
            converged
        };
        accumulator = accumulator.wrapping_add(self.length);

        let mut remaining = &self.buffer[..self.buffered];
        while let Some((lane, tail)) = remaining.split_first_chunk::<8>() {
            accumulator ^= round(0, u64::from_le_bytes(*lane));
            accumulator = accumulator
                .rotate_left(27)
                .wrapping_mul(PRIME64_1)
                .wrapping_add(PRIME64_4);
            remaining = tail;
        }
        while let Some((lane, tail)) = remaining.split_first_chunk::<4>() {
            accumulator ^= u64::from(u32::from_le_bytes(*lane)).wrapping_mul(PRIME64_1);
            accumulator = accumulator
                .rotate_left(23)
                .wrapping_mul(PRIME64_2)
                .wrapping_add(PRIME64_3);
            remaining = tail;
        }
        for byte in remaining {
            accumulator ^= u64::from(*byte).wrapping_mul(PRIME64_5);
            accumulator = accumulator.rotate_left(11).wrapping_mul(PRIME64_1);
        }

        accumulator ^= accumulator >> 33;
        accumulator = accumulator.wrapping_mul(PRIME64_2);
        accumulator ^= accumulator >> 29;
        accumulator = accumulator.wrapping_mul(PRIME64_3);
        accumulator ^= accumulator >> 32;
        accumulator
    }
}

const fn round(accumulator: u64, lane: u64) -> u64 {
    accumulator
        .wrapping_add(lane.wrapping_mul(PRIME64_2))
        .rotate_left(31)
        .wrapping_mul(PRIME64_1)
}

/// Render a checksum as fixed-width lowercase hex.
///
/// Fixed width matters: the value is serialized into artifact receipts, and a
/// decimal `u64` would make a receipt's byte length depend on its payload's
/// content. Evidence accounting compares those lengths.
pub(crate) fn hex(value: u64) -> String {
    format!("{value:016x}")
}

/// The checksum of a complete payload already held in memory.
#[cfg(test)]
pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    let mut checksum = Checksum::new();
    checksum.update(bytes);
    checksum.finish()
}

#[cfg(test)]
mod tests {
    use super::{Checksum, checksum, hex};

    #[test]
    fn hex_rendering_is_fixed_width() {
        assert_eq!(hex(0), "0000000000000000");
        assert_eq!(hex(u64::MAX), "ffffffffffffffff");
        assert_eq!(hex(checksum(&[])), "ef46db3751d8e999");
    }

    // Vectors published by the xxHash project for XXH64 with seed 0. They pin
    // this implementation to the reference C implementation, so a future edit
    // that quietly changes the primitive cannot pass.
    #[test]
    fn matches_the_reference_implementation() {
        assert_eq!(checksum(&[]), 0xef46_db37_51d8_e999);
        assert_eq!(checksum(&[42]), 0x0a9e_dece_beb0_3ae4);
        assert_eq!(checksum(b"Hello, world!\0"), 0x7b06_c531_ea43_e89f);
        let counted: [u8; 100] = std::array::from_fn(|index| u8::try_from(index).unwrap());
        assert_eq!(checksum(&counted), 0x6ac1_e580_3216_6597);
    }

    #[test]
    fn chunking_does_not_change_the_checksum() {
        let payload: Vec<u8> = (0..1021_u32)
            .map(|value| u8::try_from(value % 251).unwrap())
            .collect();
        let whole = checksum(&payload);
        for chunk in [1_usize, 3, 7, 8, 31, 32, 33, 64, 257] {
            let mut streamed = Checksum::new();
            for part in payload.chunks(chunk) {
                streamed.update(part);
            }
            assert_eq!(streamed.finish(), whole, "chunk={chunk}");
        }
    }

    #[test]
    fn a_single_flipped_bit_changes_the_checksum() {
        let payload: Vec<u8> = (0..4096_u32)
            .map(|value| u8::try_from(value % 251).unwrap())
            .collect();
        let original = checksum(&payload);
        for index in [0_usize, 1, 1023, 2048, 4095] {
            let mut mutated = payload.clone();
            mutated[index] ^= 1;
            assert_ne!(checksum(&mutated), original, "index={index}");
        }
    }
}
