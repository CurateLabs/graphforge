# Canonical bytes and fingerprints v1

**Status:** Frozen for v0.5.0
**Contract:** `graphforge-canonical/1`

**Golden vectors:** [`canonical-fingerprint-v1.json`](../../contracts/canonical-fingerprint-v1.json)

This specification defines bytes first. SHA-256, deterministic UUIDs, and
cross-language parity are downstream operations over those bytes. Rust is the
production owner; Python and Node consume the same Rust behavior and the same
language-neutral vectors.

## Invariants

- Canonicalization is a pure function of a registered contract version and
  logical values. Allocation layout, Arrow buffers, dictionary IDs, batch
  boundaries, process, host, locale, and language do not participate.
- Public result order is preserved. Canonicalization never sorts rows to hide
  an ordering bug.
- Unordered maps sort entries; ordered lists and result rows do not.
- Invalid, unsupported, oversized, or ambiguous values fail structurally.
  Canonicalization never drops a field, substitutes null, or hashes a
  diagnostic string.
- Version 1 is immutable. A changed byte rule requires a new version and new
  domain/version pair; stored v1 bytes and fingerprints remain reproducible.

## Primitive encodings

All counts and lengths are unsigned big-endian. All signed integers are
two's-complement big-endian at their declared width. A `bytes` item is a
`u64` byte length followed by that many bytes. A `text` item is a `bytes` item
whose payload is valid UTF-8.

There is no platform-native integer, varint, JSON number, locale-sensitive
number, or NUL-terminated string in the format.

### Presence

Every field, list element, struct child, and map value begins with one byte:

| Byte | Meaning |
|---|---|
| `00` | null |
| `01` | present; the type-specific bytes follow |

`00` for a non-nullable position is `GF_CANONICAL_NULLABILITY`. Any other
presence byte is `GF_CANONICAL_ENCODING`.

### UUID

A UUID value is exactly 16 network-order bytes. Ordering is unsigned
lexicographic order over those bytes. Text input accepts only the RFC 9562
8-4-4-4-12 hyphenated form, case-insensitively; it is parsed to bytes and
rendered as lowercase. URNs, braces, omitted hyphens, whitespace, and
non-RFC variants are rejected rather than repaired.

### UTF-8 and binary

UTF-8 text is preserved byte-for-byte after validation. Version 1 performs no
Unicode NFC, NFD, case folding, trimming, newline conversion, or locale
normalization. Consequently, precomposed `é` and `e` plus combining acute are
different values.

Binary values preserve exact bytes. Fixed-size binary additionally requires
the declared width.

### Integers, decimal, and temporal values

- Signed and unsigned integers use their Arrow width: 8, 16, 32, or 64 bits.
- Decimal128 and Decimal256 use exactly 16 or 32 two's-complement bytes.
  Precision and scale are schema bytes. A value outside the declared
  precision is rejected.
- Date32 and Date64 canonicalize to signed `i32` days since Unix epoch. Date64
  must be an exact UTC-millisecond multiple of one day.
- Time32 and Time64 canonicalize to unsigned `u64` microseconds since midnight.
  Conversion must be exact and the value must be in `0..86_400_000_000`.
- Duration canonicalizes to signed `i64` microseconds; conversion from
  seconds/milliseconds/nanoseconds must be exact.
- Timestamp canonicalizes to signed `i64` microseconds since Unix epoch.
  Conversion must be exact. The Arrow timezone must be absent or one of
  `UTC`, `Etc/UTC`, `Z`, or `+00:00`; all four explicit forms canonicalize to
  `UTC`. Non-UTC zones and offsets are rejected.
- Month-day-nanosecond and year-month intervals retain their declared Arrow
  components using fixed signed widths. Day-time intervals encode signed
  `i32` days followed by signed `i32` milliseconds.

Overflow or a sub-microsecond remainder returns `GF_CANONICAL_TEMPORAL`.

### Floating point

Float16, Float32, and Float64 encode their IEEE-754 bits in big-endian order
after these transformations:

1. `-0` becomes `+0`.
2. Every NaN, including signaling NaNs and every sign/payload, becomes the
   positive quiet NaN `0x7e00`, `0x7fc00000`, or
   `0x7ff8000000000000` for its width.
3. Positive and negative infinity remain distinct and are allowed unless the
   owning record schema rejects them.
4. Every other finite bit pattern is preserved.

Owning contracts may require finite values. That validation occurs before
canonical encoding and does not change the generic floating rule.

## Canonical schema bytes

Canonical schema bytes begin with ASCII `GFS1`, followed by:

```text
u32 field_count
field[field_count]
metadata
```

A field is:

```text
text name
u8 nullable                 # 00 or 01
data_type
metadata
```

Metadata is `u32 entry_count`, then `(text key, text value)` entries sorted by
raw UTF-8 key bytes. Keys beginning `graphforge.contract.` participate. The
following existing result-contract keys also participate when declared by the
owning schema registry:

```text
graphforge.algorithm
graphforge.algorithm_version
graphforge.algorithm_schema_version
graphforge.verb
graphforge.search_schema_version
graphforge.dimensions
graphforge.seed
```

`graphforge.query_id`, `graphforge.ontology_mode`,
`graphforge.ontology_version`, writer/build identity, timestamps, comments, and
non-GraphForge metadata are excluded. Extension metadata is consumed by the
extension rule below and is not duplicated. An unknown `graphforge.*` key is
rejected until its owning registry classifies it as semantic or volatile; it is
never silently excluded. Duplicate metadata keys, invalid UTF-8, or a semantic
key not declared by the owning schema is rejected.

Field order, field name bytes, nullability, normalized data type, and
participating metadata are significant. Display names, producer/build
metadata, comments, and allocation hints are not.

### Data-type tags

The first byte is the type tag; parameters follow as shown.

| Tag | Canonical type | Parameters |
|---|---|---|
| `01` | Null | none |
| `02` | Boolean | none |
| `10`–`13` | Int8/16/32/64 | none |
| `14`–`17` | UInt8/16/32/64 | none |
| `20`–`22` | Float16/32/64 | none |
| `30` | UTF-8 | none |
| `31` | Binary | none |
| `32` | Fixed-size binary | `u32 width` |
| `40` | Decimal128 | `u8 precision`, `i8 scale` |
| `41` | Decimal256 | `u8 precision`, `i8 scale` |
| `50` | Date | none |
| `51` | Time-microsecond | none |
| `52` | Timestamp-microsecond | `u8 timezone_kind`: `00` absent, `01` UTC |
| `53` | Duration-microsecond | none |
| `54` | Interval year-month | none |
| `55` | Interval day-time | none |
| `56` | Interval month-day-nanosecond | none |
| `60` | List | canonical child field |
| `61` | Fixed-size list | `u32 length`, canonical child field |
| `62` | Struct | `u32 child_count`, canonical child fields |
| `63` | Map | canonical key field, canonical value field |
| `70` | Registered extension | `text name`, `u32 version`, storage data type |

These physical Arrow distinctions normalize to one logical tag:

- Utf8 and LargeUtf8 → `30`;
- Binary and LargeBinary → `31`;
- Date32 and exact Date64 → `50`;
- all exact Time32/Time64 units → `51`;
- all exact Timestamp units → `52`;
- all exact Duration units → `53`;
- List and LargeList → `60`;
- Dictionary → its canonical decoded value type.

Dictionary index type, dictionary ID, dictionary ordering, and the physical
dictionary itself are excluded. Values are decoded logically before encoding.
A missing dictionary key or invalid index is rejected.

Arrow `Union`, `RunEndEncoded`, `Utf8View`, `BinaryView`, `ListView`,
`LargeListView`, and implementation-specific opaque types are unsupported in
v1 and return `GF_CANONICAL_ARROW_TYPE`. Supporting one requires a later
canonical contract version.

An Arrow extension is accepted only when its name and version appear in the
closed GraphForge extension registry. Its storage type must equal the
registered canonical storage type. Unregistered extensions, storage mismatch,
or extension metadata without a version is rejected. Extension values encode
using the registered storage representation.

## Canonical logical values

After the presence byte:

- fixed-width primitives use the primitive bytes above;
- UTF-8 and binary use `bytes`;
- a list uses `u64 element_count` followed by elements in logical order;
- a fixed-size list omits the count and encodes exactly its declared number of
  elements;
- a struct encodes children in schema field order;
- a map uses `u64 entry_count`, then key/value pairs sorted by the complete
  canonical key value bytes (including presence).

Map keys must be non-null. Two keys with identical canonical bytes are a
duplicate and return `GF_CANONICAL_MAP_KEY`. Arrow's physical `keys_sorted`
flag is ignored because v1 performs and verifies its own canonical sort. If
entry order has domain meaning, the schema must use a list of structs rather
than Map.

Offsets must be in bounds and monotonic for the physical layout being decoded.
Sliced arrays encode only their logical slice. Buffer padding, offset width,
null-buffer representation, and unused bytes never participate.

## Canonical table bytes

An Arrow table canonicalizes to:

```text
ASCII "GFT1"
u64 schema_byte_length
canonical_schema_bytes
u64 row_count
row_values
```

Rows encode in public result order; fields encode in schema order. RecordBatch
boundaries are ignored: batches are logically concatenated without separators.
Zero-column tables retain their row count. Empty and all-null arrays therefore
remain unambiguous.

For a result whose contract declares a canonical sort, the producer must sort
and validate before calling the encoder. The encoder preserves what it
receives. A different order produces different bytes and a different result
fingerprint.

Persisted domain records use the same table grammar with the authoritative
one-row or multi-row schema from their owning registry. A descriptor is a
registered one-row struct/table, not ad hoc JSON.

## Fingerprint envelope

The fingerprint preimage is:

```text
ASCII "GFFP"
u8 1                         # envelope version
u16 domain_tag_byte_length
domain_tag_utf8
u32 canonical_contract_version
u64 canonical_payload_length
canonical_payload
```

The fingerprint is the 32 raw bytes of SHA-256 over that entire preimage and
is rendered as 64 lowercase hexadecimal characters. Lengths make concatenation
unambiguous. The following domain tags are closed for v1:

| Domain tag | Payload |
|---|---|
| `graphforge/schema` | canonical schema bytes |
| `graphforge/assertion` | canonical assertion record/table |
| `graphforge/evidence-link` | canonical evidence-link record/table |
| `graphforge/confidence-assessment` | canonical assessment record/table |
| `graphforge/provenance-event` | canonical provenance event/table |
| `graphforge/lineage` | canonical lineage record/table |
| `graphforge/invocation-descriptor` | neutral invocation descriptor table |
| `graphforge/graph-projection` | resolved graph projection table |
| `graphforge/arrow-result` | canonical public result table |

Domain strings are exact lowercase ASCII and are not caller input. Reusing a
tag for a different payload contract is forbidden. Unknown tags or contract
versions fail before hashing.

SHA-256 collision handling is fail-closed. If an existing fingerprint key or
derived UUID is associated with different canonical bytes, the operation
returns `GF_FINGERPRINT_COLLISION`; it does not deduplicate or overwrite.

### Deterministic UUIDv8 projection

When a record contract calls for a content-derived UUID, compute its
domain-separated 32-byte fingerprint, take the first 16 bytes, set the high
nibble of byte 6 to `8`, and set the high two bits of byte 8 to `10`. The
remaining 122 bits are unchanged. Render the result in lowercase RFC form.

The full 32-byte fingerprint and canonical contract version must be stored
beside every derived UUID. Equality is confirmed by the full fingerprint, not
the shortened UUID. A record family must explicitly declare whether its UUID
is caller-supplied, UUIDv7 event identity, or derived UUIDv8; callers cannot
select the mode dynamically.

## Limits and errors

Canonicalization checks limits before allocating:

| Limit | v1 maximum |
|---|---:|
| nesting depth | 32 |
| fields per struct/schema | 4,096 |
| metadata entries per field/schema | 256 |
| UTF-8 value | 16 MiB |
| binary value | 64 MiB |
| elements in one list/map | 10,000,000 |
| rows in one canonical table | 100,000,000 |
| canonical payload | 2 GiB |

An owning API may impose a lower limit. Length arithmetic is checked. Errors
contain only stable code, domain tag, contract version, type/path position,
observed size, and limit. They never include text/binary values, map keys,
properties, evidence, reasoning, vectors, or canonical payload bytes.

## Golden-vector requirements

The JSON vectors are normative and versioned. Each vector contains:

- domain tag and canonical contract version;
- logical source variants that must converge, when applicable;
- exact canonical payload hex;
- exact fingerprint preimage hex;
- exact lowercase SHA-256; and
- derived UUIDv8 where requested.

Every Rust, Python, and Node implementation must:

1. rebuild canonical payload bytes from each logical source variant;
2. compare bytes before comparing hashes;
3. rebuild the envelope rather than hashing fixture preimage blindly;
4. reproduce the SHA-256 and UUIDv8; and
5. prove negative vectors return the specified stable error.

Required positive cases cover UUID text case, dictionary/plain equality,
RecordBatch chunk independence, negative zero, NaN payload collapse, excluded
metadata, unordered map ordering, Unicode non-normalization, and
order-significant result rows. Negative cases cover every rejected Arrow type,
invalid UTF-8, sub-microsecond temporal values, duplicate canonical map keys,
unknown extension, nullability, overflow, and each limit.

## Evolution

`graphforge-canonical/1` is never edited to reinterpret stored bytes. A future
version:

- uses a new canonical contract version in the envelope;
- publishes separate golden vectors;
- registers reader support explicitly;
- never relabels a v1 fingerprint or UUID as the new version; and
- compares or deduplicates only within the same domain and version unless a
  separately versioned, explicit conversion is performed.

The epistemic layer adds record domains and schemas but continues to reference frozen knowledge-layer
fingerprints by `(domain, version, sha256)`.
