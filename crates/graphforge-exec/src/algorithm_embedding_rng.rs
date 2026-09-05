//! Version-pinned deterministic random substreams for embedding algorithms.

const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const INITIAL_STATE: u64 = 0x6a09_e667_f3bc_c909;
const DOMAIN: &str = "graphforge-embedding-substream-v1";

/// One typed phase field in an embedding substream key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EmbeddingRngField<'a> {
    Utf8(&'a str),
    U64(u64),
    Uuid([u8; 16]),
    Bytes(&'a [u8]),
}

/// A `splitmix64-v1` stream derived using the embedding-v1 typed framing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EmbeddingRng {
    state: u64,
}

impl EmbeddingRng {
    pub(crate) fn derive(
        algorithm: &str,
        phase: &str,
        seed: u64,
        fields: &[EmbeddingRngField<'_>],
    ) -> Self {
        let mut encoded = Vec::new();
        encode_field(&mut encoded, 0x01, DOMAIN.as_bytes());
        encode_field(&mut encoded, 0x01, algorithm.as_bytes());
        encode_field(&mut encoded, 0x01, phase.as_bytes());
        encode_field(&mut encoded, 0x02, &seed.to_be_bytes());
        for field in fields {
            match field {
                EmbeddingRngField::Utf8(value) => {
                    encode_field(&mut encoded, 0x01, value.as_bytes());
                }
                EmbeddingRngField::U64(value) => {
                    encode_field(&mut encoded, 0x02, &value.to_be_bytes());
                }
                EmbeddingRngField::Uuid(value) => encode_field(&mut encoded, 0x03, value),
                EmbeddingRngField::Bytes(value) => encode_field(&mut encoded, 0x04, value),
            }
        }

        let mut state = INITIAL_STATE;
        for chunk in encoded.chunks(8) {
            let mut bytes = [0_u8; 8];
            bytes[..chunk.len()].copy_from_slice(chunk);
            state = mix(state ^ u64::from_be_bytes(bytes)).wrapping_add(GAMMA);
        }
        Self { state }
    }

    #[cfg(test)]
    pub(crate) fn derived_state(&self) -> u64 {
        self.state
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        mix(self.state)
    }

    /// Return exactly 52 random mantissa bits in `[0, 1)`.
    pub(crate) fn unit_f64(&mut self) -> f64 {
        f64::from_bits(0x3ff0_0000_0000_0000 | (self.next() >> 12)) - 1.0
    }

    /// Draw uniformly from `[0, upper)` using rejection rather than modulo bias.
    pub(crate) fn bounded(&mut self, upper: u64) -> Option<u64> {
        if upper == 0 {
            return None;
        }
        let threshold = upper.wrapping_neg() % upper;
        loop {
            let draw = self.next();
            if draw >= threshold {
                return Some(draw % upper);
            }
        }
    }
}

fn encode_field(output: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    output.push(tag);
    output.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    output.extend_from_slice(payload);
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{EmbeddingRng, EmbeddingRngField};

    #[test]
    fn authoritative_embedding_substream_goldens_are_exact() {
        let node = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let fields = [
            EmbeddingRngField::Uuid(node),
            EmbeddingRngField::U64(0),
            EmbeddingRngField::U64(0),
        ];
        assert_golden(
            EmbeddingRng::derive("node2vec", "walk", 0, &fields),
            0x0b8a_2ce4_1ee3_72c1,
            0xde12_f263_45fe_f4de,
            0.867_476_605_641_055_2,
        );

        let fields = [
            EmbeddingRngField::Uuid([0xff; 16]),
            EmbeddingRngField::U64(7),
        ];
        assert_golden(
            EmbeddingRng::derive("fastrp", "node-projection", 42, &fields),
            0x73d5_0f07_7714_78be,
            0xbcf4_d12c_f3fc_ae14,
            0.738_110_612_368_461_1,
        );
    }

    fn assert_golden(mut rng: EmbeddingRng, state: u64, next: u64, unit: f64) {
        assert_eq!(rng.derived_state(), state);
        assert_eq!(rng.next(), next);
        let mut fresh = EmbeddingRng { state };
        assert_eq!(fresh.unit_f64().to_bits(), unit.to_bits());
    }

    #[test]
    fn typed_fields_and_domains_separate_substreams() {
        let base = EmbeddingRng::derive("hashgnn", "phase", 9, &[EmbeddingRngField::U64(1)]);
        assert_ne!(
            base,
            EmbeddingRng::derive("fastrp", "phase", 9, &[EmbeddingRngField::U64(1)])
        );
        assert_ne!(
            base,
            EmbeddingRng::derive("hashgnn", "other", 9, &[EmbeddingRngField::U64(1)])
        );
        assert_ne!(
            base,
            EmbeddingRng::derive("hashgnn", "phase", 10, &[EmbeddingRngField::U64(1)])
        );
        assert_ne!(
            EmbeddingRng::derive("hashgnn", "phase", 9, &[EmbeddingRngField::Utf8("x")]),
            EmbeddingRng::derive("hashgnn", "phase", 9, &[EmbeddingRngField::Bytes(b"x")]),
        );
    }

    #[test]
    fn unit_and_bounded_obey_their_ranges_and_replay() {
        let mut left = EmbeddingRng::derive("graphsage", "test", 3, &[]);
        let mut right = left.clone();
        for _ in 0..1_000 {
            assert_eq!(left.bounded(7), right.bounded(7));
            let unit = left.unit_f64();
            assert_eq!(unit.to_bits(), right.unit_f64().to_bits());
            assert!((0.0..1.0).contains(&unit));
        }
        assert_eq!(left.bounded(0), None);
    }
}
