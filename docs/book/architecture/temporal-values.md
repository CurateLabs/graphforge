# Temporal value contract

GraphForge owns temporal behavior in Rust and transports temporal graph data as
typed Arrow values. Python, Node, the CLI, and visualization consumers receive
the same Arrow schema and buffers; they must not reconstruct temporal meaning.

| Cypher value | Rust `TemporalValue` | Canonical Arrow / Parquet |
|---|---|---|
| `date` | `Date { epoch_days }` | `Struct<epoch_day: Int64>` |
| `localtime` | `LocalTime { nanos }` | `Time64(Nanosecond)` |
| offset `time` | `OffsetTime { nanos, offset_seconds }` | `Struct<time: Time64(Nanosecond), offset: Int32>` |
| `localdatetime` | `LocalDateTime { epoch_days, nanos }` | `Struct<date: Int64, time: Time64(Nanosecond)>` |
| UTC instant | `UtcDateTime { epoch_micros }` | `Timestamp(Microsecond, UTC)` |
| offset/zoned `datetime` | `ZonedDateTime { epoch_days, nanos, offset_seconds, zone }` | `Struct<date: Int64, time: Time64(Nanosecond), offset: Int32, zone: Utf8?>` |
| `duration` | `Duration { months, days, seconds, nanos }` | `Struct<months: Int64, days: Int64, seconds: Int64, nanos: Int64>` |

All structures are nullable at the property boundary. Child fields are
non-null for a present value except `zone`, whose null means an offset-only
datetime. A zone string is preserved byte-for-byte as identity metadata; the
stored offset is also retained because it disambiguates daylight-saving
overlaps. Unknown zones may be transported, but an operation that needs zone
rules must return a typed unsupported-zone error.

Nanosecond wall-clock precision and microsecond UTC-instant precision are the
only accepted units. Bulk ingestion rejects alternate units, naive timestamp
columns, malformed structures, out-of-range offsets, invalid wall-clock nanos,
and partial child nulls before publication. Calendar duration fields remain
independent signed integers and are never reduced to a fixed elapsed duration.

Arithmetic, comparison, and truncation use the same Rust Cypher implementation
for stored columns, query parameters, and literals. Host-local timezone
defaults are not part of the contract. DST gaps, overlaps, leap days, and range
overflow therefore have deterministic outcomes or stable typed errors rather
than environment-dependent normalization.
