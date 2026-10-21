//! Canonical tar bytes for tests: headers, PAX-addressed entries, padding.

use super::{ContractSha256, canonical_ustar_split, hex, pax};
use sha2::Digest;

fn octal(field: &mut [u8], value: u64) {
    field.fill(0);
    let digits = format!("{:0width$o}", value, width = field.len() - 1);
    field[..digits.len()].copy_from_slice(digits.as_bytes());
}
pub(super) fn tar_entry(path: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = tar_header(path, payload.len() as u64);
    out.extend_from_slice(payload);
    out.resize(out.len() + ((512 - payload.len() % 512) % 512), 0);
    out
}
/// The ustar header of a regular entry of `size` bytes at `path`.
pub(super) fn tar_header(path: &str, size: u64) -> Vec<u8> {
    let mut h = [0u8; 512];
    let (prefix, name) = canonical_ustar_split(path).unwrap();
    h[..name.len()].copy_from_slice(name.as_bytes());
    h[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
    octal(&mut h[100..108], 0o644);
    octal(&mut h[108..116], 0);
    octal(&mut h[116..124], 0);
    octal(&mut h[124..136], size);
    octal(&mut h[136..148], 0);
    h[148..156].fill(b' ');
    h[156] = b'0';
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    let sum: u64 = h.iter().map(|b| *b as u64).sum();
    let checksum = format!("{:06o}\0 ", sum);
    h[148..156].copy_from_slice(checksum.as_bytes());
    h.to_vec()
}

/// One raw header with an explicit name, typeflag and `size` field.
pub(super) fn raw_header(name: &str, kind: u8, size_field: u64) -> Vec<u8> {
    let mut h = [0u8; 512];
    h[..name.len()].copy_from_slice(name.as_bytes());
    octal(&mut h[100..108], 0o644);
    octal(&mut h[108..116], 0);
    octal(&mut h[116..124], 0);
    octal(&mut h[124..136], size_field);
    octal(&mut h[136..148], 0);
    h[148..156].fill(b' ');
    h[156] = kind;
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
    h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    h.to_vec()
}

pub(super) fn padded(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.resize(bytes.len().div_ceil(512) * 512, 0);
    bytes
}

/// Append `payload` at `path` as the writer would: a ustar header when the
/// path fits, otherwise a local PAX path record and a placeholder name.
pub(super) fn append_entry(bundle: &mut Vec<u8>, path: &str, payload: Vec<u8>) {
    if canonical_ustar_split(path).is_some() {
        bundle.extend(tar_entry(path, &payload));
        return;
    }
    let records = pax::record("path", path);
    let suffix = &hex(&ContractSha256::digest(path.as_bytes()))[..16];
    bundle.extend(raw_header(
        &format!("PaxHeaders/{suffix}"),
        b'x',
        records.len() as u64,
    ));
    bundle.extend(padded(records.into_bytes()));
    bundle.extend(raw_header(
        &format!("PaxFiles/{suffix}"),
        b'0',
        payload.len() as u64,
    ));
    bundle.extend(padded(payload));
}
