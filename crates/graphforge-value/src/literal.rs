//! Compiler-independent literal carrier with the existing tagged JSON encoding.

use graphforge_core::SpatialValue;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------------------
// Literal
// ---------------------------------------------------------------------------

/// A scalar constant value in the Graph IR.
///
/// Temporal values are stored as microseconds since the Unix epoch (UTC),
/// consistent with the Arrow `Timestamp(Microsecond, "UTC")` convention used
/// throughout the project.
///
/// ## Float serialisation
///
/// Finite `Float` values serialise as JSON numbers.  Non-finite IEEE-754 values
/// (`NaN`, `+Infinity`, `-Infinity`) that `serde_json` cannot represent as JSON
/// numbers are encoded as a tagged object `{"$float": "<tag>"}` where `<tag>` is
/// `"NaN"`, `"+Infinity"`, or `"-Infinity"`.  This preserves full round-trip
/// fidelity rather than silently collapsing to `null`.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// The Cypher `null` value.
    Null,
    /// A boolean constant.
    Bool(bool),
    /// A 64-bit integer constant.
    Int(i64),
    /// A 64-bit floating-point constant.
    ///
    /// Non-finite values (NaN, ±Infinity) are supported and round-trip through
    /// JSON via a tagged encoding; see the enum-level docs.
    Float(f64),
    /// A UTF-8 string constant.
    Str(String),
    /// A typed UUID query parameter, stored as its canonical 16-byte identity.
    /// This is not Cypher syntax and must not be inferred from strings.
    Uuid([u8; 16]),
    /// A Cypher duration as signed months/days/nanos (ADR 0009): months and days
    /// kept distinct from sub-day time. Persisted as a `Struct{months,days,nanos}`
    /// (Parquet cannot store Arrow `Interval`). (#920)
    Duration {
        /// Signed whole months.
        months: i64,
        /// Signed whole days.
        days: i64,
        /// Signed whole sub-day seconds (split from `nanos` so billion-year spans
        /// fit `i64`). (#1011)
        seconds: i64,
        /// Signed nanoseconds-of-second, `(-1e9, 1e9)`, same sign as `seconds`.
        nanos: i64,
    },
    /// A point-in-time expressed as microseconds since the Unix epoch (UTC).
    DateTime(i64),
    /// A calendar date as **i64 days** since the Unix epoch — the full openCypher
    /// year range −999,999,999..+999,999,999. Persisted as a self-describing
    /// `Struct{epoch_day: Int64}` (a bare Int64 is indistinguishable from an
    /// integer property). (#920/#1011)
    Date(i64),
    /// A Cypher `localdatetime` (ADR 0009): a date (`days` since the Unix epoch)
    /// plus a time-of-day (`nanos` since midnight), with no zone. Persisted as a
    /// `Struct{date: Int64, time: Time64(ns)}`. (#920/#1011)
    LocalDateTime {
        /// Days since the Unix epoch (i64, full year range).
        days: i64,
        /// Nanoseconds since midnight (Arrow `Time64(ns)`).
        nanos: i64,
    },
    /// A Cypher `localtime` (ADR 0009): a time-of-day in nanoseconds since
    /// midnight, no zone. Persisted as a native Arrow `Time64(ns)` column. (#920)
    Time(i64),
    /// A Cypher `time` (ADR 0009): a time-of-day plus its UTC offset in seconds.
    /// Persisted as a `Struct{time: Time64(ns), offset: Int32}`. (#920)
    ZonedTime {
        /// Nanoseconds since midnight (Arrow `Time64(ns)`).
        nanos: i64,
        /// UTC offset in seconds.
        offset: i32,
    },
    /// A Cypher `datetime` (ADR 0009): a date+time, its UTC offset in seconds,
    /// and an optional named IANA zone. Persisted as a
    /// `Struct{date: Date32, time: Time64(ns), offset: Int32, zone: Utf8}`.
    /// Distinct from [`Literal::DateTime`] (a bare UTC micros instant). (#920/#1011)
    ZonedDateTime {
        /// Days since the Unix epoch (i64, full year range).
        days: i64,
        /// Nanoseconds since midnight (Arrow `Time64(ns)`).
        nanos: i64,
        /// UTC offset in seconds.
        offset: i32,
        /// Named IANA zone, or `None` for an offset-only datetime.
        zone: Option<String>,
    },
    /// A canonical typed spatial property value.
    Spatial(SpatialValue),
    /// A homogeneous list of values, persisted as an Arrow `List<inner>` column
    /// (the inner type is inferred from the elements). Stores e.g. a property
    /// whose value is `[date(…), date(…)]`. Heterogeneous lists are out of scope
    /// (#1005). (#1006)
    List(Vec<Literal>),
    /// A query-parameter map value. Map literals in parsed Cypher still lower as
    /// `IrExpr::MapLiteral`; this variant lets callers bind `$param` to a map
    /// through `execute_with_params` and then use Cypher value access on it.
    Map(Vec<(String, Literal)>),
}

// ---------------------------------------------------------------------------
// Literal — custom Serialize
// ---------------------------------------------------------------------------

impl Serialize for Literal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Delegate all variants except Float to the derived representation by
        // using an internal helper that derives Serialize on a mirrored enum.
        match self {
            Self::Null => LiteralSer::Null.serialize(s),
            Self::Bool(b) => LiteralSer::Bool(*b).serialize(s),
            Self::Int(i) => LiteralSer::Int(*i).serialize(s),
            Self::Str(v) => LiteralSer::Str(v).serialize(s),
            Self::Uuid(v) => LiteralSer::Uuid(v).serialize(s),
            Self::Duration {
                months,
                days,
                seconds,
                nanos,
            } => LiteralSer::Duration(*months, *days, *seconds, *nanos).serialize(s),
            Self::DateTime(dt) => LiteralSer::DateTime(*dt).serialize(s),
            Self::Date(d) => LiteralSer::Date(*d).serialize(s),
            Self::LocalDateTime { days, nanos } => {
                LiteralSer::LocalDateTime(*days, *nanos).serialize(s)
            }
            Self::Time(n) => LiteralSer::Time(*n).serialize(s),
            Self::ZonedTime { nanos, offset } => {
                LiteralSer::ZonedTime(*nanos, *offset).serialize(s)
            }
            Self::ZonedDateTime {
                days,
                nanos,
                offset,
                zone,
            } => LiteralSer::ZonedDateTime(*days, *nanos, *offset, zone.clone()).serialize(s),
            Self::Spatial(value) => LiteralSer::Spatial(value).serialize(s),
            // Each element serialises via `Literal`'s own impl (so nested
            // non-finite floats keep their tagged encoding).
            Self::List(items) => LiteralSer::List(items).serialize(s),
            Self::Map(entries) => LiteralSer::Map(entries).serialize(s),
            Self::Float(f) => {
                if f.is_finite() {
                    LiteralSer::Float(*f).serialize(s)
                } else {
                    // Encode non-finite as {"$float": "<tag>"}
                    let tag = if f.is_nan() {
                        "NaN"
                    } else if *f > 0.0 {
                        "+Infinity"
                    } else {
                        "-Infinity"
                    };
                    let mut map = s.serialize_map(Some(1))?;
                    map.serialize_entry("$float", tag)?;
                    map.end()
                }
            }
        }
    }
}

/// Internal mirror enum used purely to drive derived `Serialize` for the
/// finite/non-float variants of [`Literal`].
#[derive(Serialize)]
#[serde(tag = "type", content = "value")]
enum LiteralSer<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    Uuid(&'a [u8; 16]),
    Duration(i64, i64, i64, i64),
    DateTime(i64),
    Date(i64),
    LocalDateTime(i64, i64),
    Time(i64),
    ZonedTime(i64, i32),
    ZonedDateTime(i64, i64, i32, Option<String>),
    Spatial(&'a SpatialValue),
    List(&'a [Literal]),
    Map(&'a [(String, Literal)]),
}

// ---------------------------------------------------------------------------
// Literal — custom Deserialize
// ---------------------------------------------------------------------------

impl<'de> Deserialize<'de> for Literal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(LiteralVisitor)
    }
}

struct LiteralVisitor;

impl<'de> Visitor<'de> for LiteralVisitor {
    type Value = Literal;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "an IrLiteral (tagged object with \"type\"/\"value\" fields, \
             or {{\"$float\": \"NaN\"/\"+Infinity\"/\"-Infinity\"}})"
        )
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        // Two shapes are valid:
        //   {"$float": "<tag>"}  — non-finite float
        //   {"type": "<Variant>", "value": <data>}  — everything else

        let first_key: String = map
            .next_key()?
            .ok_or_else(|| de::Error::custom("expected at least one map key"))?;

        if first_key == "$float" {
            let tag: String = map.next_value()?;
            let f = match tag.as_str() {
                "NaN" => f64::NAN,
                "+Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                other => {
                    return Err(de::Error::unknown_variant(
                        other,
                        &["NaN", "+Infinity", "-Infinity"],
                    ));
                }
            };
            return Ok(Literal::Float(f));
        }

        if first_key != "type" {
            return Err(de::Error::unknown_field(&first_key, &["type", "$float"]));
        }

        let variant: String = map.next_value()?;
        // Scan forward through remaining map entries for "value", ignoring
        // any unknown fields along the way so field-order is irrelevant.
        match variant.as_str() {
            "Null" => Ok(Literal::Null),
            "Bool" => Ok(Literal::Bool(read_value_field(&mut map)?)),
            "Int" => Ok(Literal::Int(read_value_field(&mut map)?)),
            "Float" => Ok(Literal::Float(read_value_field(&mut map)?)),
            "Str" => Ok(Literal::Str(read_value_field(&mut map)?)),
            "Uuid" => Ok(Literal::Uuid(read_value_field(&mut map)?)),
            "Duration" => {
                let (months, days, seconds, nanos) = read_value_field(&mut map)?;
                Ok(Literal::Duration {
                    months,
                    days,
                    seconds,
                    nanos,
                })
            }
            "DateTime" => Ok(Literal::DateTime(read_value_field(&mut map)?)),
            "Date" => Ok(Literal::Date(read_value_field(&mut map)?)),
            "LocalDateTime" => {
                let (days, nanos) = read_value_field(&mut map)?;
                Ok(Literal::LocalDateTime { days, nanos })
            }
            "Time" => Ok(Literal::Time(read_value_field(&mut map)?)),
            "ZonedTime" => {
                let (nanos, offset) = read_value_field(&mut map)?;
                Ok(Literal::ZonedTime { nanos, offset })
            }
            "ZonedDateTime" => {
                let (days, nanos, offset, zone) = read_value_field(&mut map)?;
                Ok(Literal::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone,
                })
            }
            "Spatial" => Ok(Literal::Spatial(read_value_field(&mut map)?)),
            "List" => Ok(Literal::List(read_value_field(&mut map)?)),
            "Map" => Ok(Literal::Map(read_value_field(&mut map)?)),
            other => Err(de::Error::unknown_variant(
                other,
                &[
                    "Null",
                    "Bool",
                    "Int",
                    "Float",
                    "Str",
                    "Uuid",
                    "Duration",
                    "DateTime",
                    "Date",
                    "LocalDateTime",
                    "Time",
                    "ZonedTime",
                    "ZonedDateTime",
                    "Spatial",
                    "List",
                    "Map",
                ],
            )),
        }
    }
}

/// Scan `map` for a key named `"value"`, skipping any unrecognised keys, and
/// deserialise its value as `T`.  Returns `de::Error::missing_field("value")`
/// if the key is not found.
fn read_value_field<'de, T, A>(map: &mut A) -> Result<T, A::Error>
where
    T: Deserialize<'de>,
    A: MapAccess<'de>,
{
    while let Some(key) = map.next_key::<String>()? {
        if key == "value" {
            return map.next_value();
        }
        let _: de::IgnoredAny = map.next_value()?;
    }
    Err(de::Error::missing_field("value"))
}
