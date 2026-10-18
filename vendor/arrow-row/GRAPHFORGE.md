# GraphForge vendoring notice

This directory contains `arrow-row` 58.4.0 from crates.io, under Apache
License 2.0. The registry crate checksum is
`514ba0ef0d4c5896202dae736251ce415abb43a950bed570fb7981b8716c0e4c`.
The source was copied from the pristine registry archive; Cargo's `.cargo-ok`
marker is omitted. `LICENSE.txt`, `NOTICE.txt`, the other Rust source files,
and `Cargo.toml.orig` retain their upstream contents.

## Borrowed ownership observations

The only source patch is additive in `src/lib.rs`. The normalized `Cargo.toml`
adds an opt-in `ownership-diagnostics` feature, disabled by default. When enabled,
`RowConverter::diagnostic_storage` and `Rows::diagnostic_storage` return numeric
observations of existing storage. They borrow fields, codecs, and row buffers;
they do not clone owners, read row values, or change the existing RowConverter,
Rows, or RowConfig layouts and charge formulas.

Rows observations distinguish buffer capacity from offset capacity, including
zero-row allocations. Converter observations report codec vector length and
capacity separately from the codec element width. Both report the shared sort
field payload address, length, byte width, and current reference count. These
addresses are observations, not allocation receipts or lifetime identifiers.
The field byte count is the slice payload size; it does not claim to describe
Arc's private allocation header or allocator bookkeeping.

Stateless codecs with allocation-free field datatypes, including primitive
one-key and two-key configurations, expose the listed storage directly.
Nested codecs and datatypes with separately allocated metadata, including
nonempty timestamp timezones, report incomplete metadata rather than an
estimated size. The accessor's completeness flag covers the datatype payload,
not provenance, funding, or lifetime completeness of a whole capture.

The feature-gated regression distinguishes equal charged sizes backed by
separate zero-row buffer and offset capacities, checks shared field identity
without increasing its reference count, and rejects nested-codec completeness.
Product controls must run through the workspace's existing Rust test lane;
workspace exclusion does not make this crate's unit tests a PR gate.

## Updating and removal

Compare `src/lib.rs` and the feature declaration against registry `arrow-row`
58.4.0 when updating. Remove this copy, its workspace exclusion, and the root
patch entry when the adopted Arrow version provides equivalent borrowed
accessors, then rerun the product ownership controls. Root Cargo patches apply
to repository builds; published manifests resolve registry dependencies.
