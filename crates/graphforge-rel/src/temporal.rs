//! Cypher temporal-value construction, evaluated at lowering time.
//!
//! The openCypher TCK constructs temporals from **literal** arguments —
//! `date('2015-W30-2')`, `date({year: 2015, month: 7, day: 21})` — and renders
//! the result as a quoted canonical ISO string (`'2015-07-21'`). Because the
//! argument is a constant, we can parse and canonicalise it during lowering
//! (`graphforge-rel`) and emit a plain `Utf8` literal, without a runtime UDF. Only the
//! non-literal forms (rare in the corpus) fall back to DataFusion's
//! `to_date`/`to_char`.
//!
//! This module currently covers `date(<string>)`. The map form
//! (`date({year, month, day})` / ISO-week construction) and the time-bearing
//! types (`localtime`/`time`/`localdatetime`/`datetime`) are follow-ups (#599).

use chrono::{NaiveDate, NaiveTime, Timelike};
use std::collections::HashMap;

/// A temporal map-constructor field value, extracted from a literal map
/// argument (`date({year: 1984, …})`) at lowering time. Variable references and
/// other non-constant values can't be extracted and make construction bail to
/// the runtime path. (#599)
pub enum TemporalField {
    /// An integer field (`year`, `hour`, `nanosecond`, …).
    Int(i64),
    /// A numeric field that may be fractional (duration `days: 1.5`).
    Float(f64),
    /// A string field (`timezone: '+01:00'` / `'Europe/Stockholm'`).
    Str(String),
    /// A nested temporal anchor (`{date: date('…'), …}`) as i64 days. (#1011)
    Date(i64),
}

/// Lowering-time map of extracted temporal fields.
type Fields = HashMap<String, TemporalField>;

fn f_int(f: &Fields, k: &str) -> Option<i64> {
    match f.get(k)? {
        TemporalField::Int(n) => Some(*n),
        _ => None,
    }
}

fn f_num(f: &Fields, k: &str) -> Option<f64> {
    #[allow(
        clippy::cast_precision_loss,
        reason = "duration counts are small; exactness beyond f64 is irrelevant"
    )]
    match f.get(k)? {
        TemporalField::Int(n) => Some(*n as f64),
        TemporalField::Float(x) => Some(*x),
        _ => None,
    }
}

fn f_str<'a>(f: &'a Fields, k: &str) -> Option<&'a str> {
    match f.get(k)? {
        TemporalField::Str(s) => Some(s),
        _ => None,
    }
}

fn f_date(f: &Fields, k: &str) -> Option<i64> {
    match f.get(k)? {
        TemporalField::Date(d) => Some(*d),
        _ => None,
    }
}

/// Canonical rendering of a temporal constructor called with a literal **map**
/// argument (`date({year: 1984, month: 10, day: 11})`), or `None` if the fields
/// don't form a valid value for `name`. (#599)
#[must_use]
pub fn render_temporal_map(name: &str, fields: &Fields) -> Option<String> {
    match name {
        "date" => resolve_date(fields).map(format_date),
        "localtime" => {
            if !fields.contains_key("hour") {
                return None;
            }
            Some(format_time(&resolve_time(fields)?))
        }
        "time" => build_time_map(fields),
        "localdatetime" => {
            let date = resolve_date(fields)?;
            let time = resolve_time(fields)?;
            Some(format!("{}T{}", format_date(date), format_time(&time)))
        }
        "datetime" => build_date_time_map(fields),
        "duration" => Some(format_duration(&build_duration_map(fields)?)),
        _ => None,
    }
}

/// Build a `date` (i64 days) from a literal map — the typed-value path
/// (ADR 0009). `None` if the fields don't form a valid date. (#1011)
#[must_use]
pub fn date_from_map(fields: &Fields) -> Option<i64> {
    resolve_date(fields)
}

/// i64 days since the Unix epoch (1970-01-01) for a chrono [`NaiveDate`]. No
/// clamping — the full range round-trips (#1011).
#[must_use]
pub fn date_to_epoch_days(d: NaiveDate) -> Option<i64> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((d - epoch).num_days())
}

/// The chrono [`NaiveDate`] for an i64 days-since-epoch value — the in-range
/// bridge for the date functions that keep chrono internals (projection,
/// truncation, accessors, map construction). Returns `None` for a date outside
/// chrono's ±262k-year range; such extreme dates only occur on the
/// parse→`duration.between` path, which uses [`crate::calendar`] directly. (#1011)
#[must_use]
pub fn epoch_days_to_date(days: i64) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::try_days(days)?)
}

/// The date component (i64 days) of a runtime temporal value for projection — a
/// date is taken directly; a string is parsed as a date or the date part of a
/// datetime (`2015-07-21T…`). (#920/#1011)
#[must_use]
pub fn parse_date_or_datetime_prefix(s: &str) -> Option<i64> {
    let head = s.split_once('T').map_or(s, |(d, _)| d);
    parse_date_string(head)
}

/// Component overrides for `date`-from-value projection (`Temporal3`). A field
/// left `None` is taken from the base date. The active construction mode is
/// selected by which override is present (week ▸ ordinal ▸ quarter ▸ calendar).
#[derive(Default)]
pub struct DateOverrides {
    /// Calendar / ISO-week / ordinal / quarter year.
    pub year: Option<i64>,
    /// Calendar month (1–12).
    pub month: Option<i64>,
    /// Calendar day of month.
    pub day: Option<i64>,
    /// ISO week of year (selects ISO-week mode).
    pub week: Option<i64>,
    /// ISO day of week (1 = Monday … 7 = Sunday).
    pub day_of_week: Option<i64>,
    /// Day of year (selects ordinal mode).
    pub ordinal_day: Option<i64>,
    /// Quarter 1–4 (selects quarter mode).
    pub quarter: Option<i64>,
    /// 1-based day within the quarter.
    pub day_of_quarter: Option<i64>,
}

/// Project a base date (i64 days) through component overrides (openCypher
/// `date({date: base, …})` select-semantics): take `base`, replace the named
/// components, keep the rest. Range-complete via `calendar` (#920/#1011).
#[must_use]
pub fn project_date(base_days: i64, o: &DateOverrides) -> Option<i64> {
    use crate::calendar;
    let (base_year, base_month, base_day) = calendar::civil_from_days(base_days);
    let year = o.year.unwrap_or(base_year);
    // ISO-week mode — selected by `week` OR `dayOfWeek`; the other defaults to
    // the base's value.
    if o.week.is_some() || o.day_of_week.is_some() {
        let (base_iso_year, base_week) = calendar::iso_week(base_days);
        let iso_year = o.year.unwrap_or(base_iso_year);
        let week = match o.week {
            Some(w) => u32::try_from(w).ok()?,
            None => base_week,
        };
        let dow = match o.day_of_week {
            Some(d) => u32::try_from(d).ok()?,
            None => calendar::iso_weekday(base_days),
        };
        return calendar::from_iso_ywd(iso_year, week, dow);
    }
    if let Some(ord) = o.ordinal_day {
        return calendar::from_ordinal(year, u32::try_from(ord).ok()?);
    }
    // Quarter mode — selected by `quarter` OR `dayOfQuarter`.
    if o.quarter.is_some() || o.day_of_quarter.is_some() {
        let quarter = match o.quarter {
            Some(q) => q,
            None => i64::from((base_month - 1) / 3 + 1),
        };
        let day_of_quarter = o
            .day_of_quarter
            .unwrap_or_else(|| base_day_of_quarter(base_days));
        // A `dayOfQuarter` below 1 is invalid (matches the old unsigned cast).
        let offset = u64::try_from(day_of_quarter - 1).ok()?;
        let start_month = u32::try_from((quarter - 1) * 3 + 1).ok()?;
        let start = calendar::ymd_to_days(year, start_month, 1)?;
        let result = start + i64::try_from(offset).ok()?;
        // Reject a `dayOfQuarter` that spills into another quarter/year.
        let (result_year, result_month, _) = calendar::civil_from_days(result);
        let result_quarter = i64::from((result_month - 1) / 3 + 1);
        if result_year != year || result_quarter != quarter {
            return None;
        }
        return Some(result);
    }
    let month = match o.month {
        Some(m) => u32::try_from(m).ok()?,
        None => base_month,
    };
    let day = match o.day {
        Some(d) => u32::try_from(d).ok()?,
        None => base_day,
    };
    calendar::ymd_to_days(year, month, day)
}

/// Whether `name` is a `date` component accessor (`d.year`, `d.weekDay`, …).
#[must_use]
pub fn is_date_accessor(name: &str) -> bool {
    matches!(
        name,
        "year"
            | "quarter"
            | "month"
            | "week"
            | "weekYear"
            | "day"
            | "ordinalDay"
            | "weekDay"
            | "dayOfQuarter"
    )
}

/// A `date` component value (openCypher `Temporal5`). ISO-8601 semantics for
/// `week`/`weekYear`/`weekDay`; `weekDay` is 1 (Monday) … 7 (Sunday). `None`
/// for an unknown accessor name.
#[must_use]
pub fn date_component(days: i64, name: &str) -> Option<i64> {
    // Range-complete via `calendar` (NOT the chrono bridge): a parsed/stored
    // extreme-year date must return its true component, not NULL. (#1011)
    use crate::calendar;
    let (year, month, day) = calendar::civil_from_days(days);
    Some(match name {
        "year" => year,
        "quarter" => i64::from((month - 1) / 3 + 1),
        "month" => i64::from(month),
        "week" => i64::from(calendar::iso_week(days).1),
        "weekYear" => calendar::iso_week(days).0,
        "day" => i64::from(day),
        "ordinalDay" => i64::from(calendar::ordinal(days)),
        "weekDay" => i64::from(calendar::iso_weekday(days)),
        "dayOfQuarter" => base_day_of_quarter(days),
        _ => return None,
    })
}

/// Whether `name` is a time-of-day component accessor (`d.hour`, `d.nanosecond`,
/// …) — valid on `localtime`/`time`/`localdatetime`/`datetime`. (#920)
#[must_use]
pub fn is_time_accessor(name: &str) -> bool {
    matches!(
        name,
        "hour" | "minute" | "second" | "millisecond" | "microsecond" | "nanosecond"
    )
}

/// Whether `name` is a zone INT accessor (`d.offsetMinutes`/`d.offsetSeconds`) —
/// valid on `time`/`datetime`. (#920)
#[must_use]
pub fn is_zone_int_accessor(name: &str) -> bool {
    matches!(name, "offsetMinutes" | "offsetSeconds")
}

/// Whether `name` is a zone STRING accessor (`d.timezone`/`d.offset`) — valid on
/// `time`/`datetime`. (#920)
#[must_use]
pub fn is_zone_str_accessor(name: &str) -> bool {
    matches!(name, "timezone" | "offset")
}

/// Whether `name` is a `datetime` epoch accessor (`d.epochSeconds`/
/// `d.epochMillis`). (#920)
#[must_use]
pub fn is_epoch_accessor(name: &str) -> bool {
    matches!(name, "epochSeconds" | "epochMillis")
}

/// A time-of-day component value (openCypher `Temporal5`) from a nanoseconds-of-day.
/// `millisecond`/`microsecond`/`nanosecond` are the CUMULATIVE sub-second value at
/// that resolution (645 / 645876 / 645876123), not the digits of a single place.
/// `None` for an unknown accessor name. (#920)
#[must_use]
pub fn time_component(nanos: i64, name: &str) -> Option<i64> {
    let sub = nanos.rem_euclid(1_000_000_000);
    Some(match name {
        "hour" => nanos.div_euclid(3_600_000_000_000) % 24,
        "minute" => nanos.div_euclid(60_000_000_000) % 60,
        "second" => nanos.div_euclid(1_000_000_000) % 60,
        "millisecond" => sub / 1_000_000,
        "microsecond" => sub / 1_000,
        "nanosecond" => sub,
        _ => return None,
    })
}

/// A zone INT component (`Temporal5`): `offsetMinutes`/`offsetSeconds` from a UTC
/// offset in seconds. `None` for an unknown accessor name. (#920)
#[must_use]
pub fn zone_int_component(offset_seconds: i32, name: &str) -> Option<i64> {
    Some(match name {
        "offsetMinutes" => i64::from(offset_seconds) / 60,
        "offsetSeconds" => i64::from(offset_seconds),
        _ => return None,
    })
}

/// A zone STRING component (`Temporal5`): `offset` (the `±HH:MM` designator) or
/// `timezone` (the named IANA zone if present, else the offset designator).
/// `None` for an unknown accessor name. (#920)
#[must_use]
pub fn zone_str_component(offset_seconds: i32, zone: Option<&str>, name: &str) -> Option<String> {
    let offset_str = || {
        format_offset(&Offset {
            seconds: offset_seconds,
            has_seconds: offset_seconds % 60 != 0,
        })
    };
    Some(match name {
        "offset" => offset_str(),
        "timezone" => zone.map_or_else(offset_str, str::to_string),
        _ => return None,
    })
}

/// A `datetime` epoch component (`Temporal5`): `epochSeconds`/`epochMillis` — the
/// UTC instant of `(date_days, nanos_of_day, offset_seconds)`. `None` for an
/// unknown accessor name. (#920)
#[must_use]
pub fn epoch_component(days: i64, nanos: i64, offset_seconds: i32, name: &str) -> Option<i64> {
    let epoch_secs = days * 86_400 + nanos.div_euclid(1_000_000_000) - i64::from(offset_seconds);
    Some(match name {
        "epochSeconds" => epoch_secs,
        "epochMillis" => epoch_secs * 1_000 + nanos.rem_euclid(1_000_000_000) / 1_000_000,
        _ => return None,
    })
}

/// Truncate a date to the start of a unit (openCypher `date.truncate(unit, …)`):
/// `millennium`/`century`/`decade` round the year down; `year`/`month`/`quarter`
/// → the first day of that period; `weekYear` → the Monday of ISO week 1;
/// `week` → the Monday of the date's week; `day` → the date itself. The optional
/// override map is applied afterwards via [`project_date`]. `None` for an unknown
/// unit. (#920)
#[must_use]
pub fn truncate_date(days: i64, unit: &str) -> Option<i64> {
    // Range-complete via `calendar` (#1011): truncating an extreme-year date must
    // yield the true period start, not NULL.
    use crate::calendar;
    let (year, month, _) = calendar::civil_from_days(days);
    let jan1 = |y: i64| calendar::ymd_to_days(y, 1, 1);
    match unit {
        "millennium" => jan1(year.div_euclid(1000) * 1000),
        "century" => jan1(year.div_euclid(100) * 100),
        "decade" => jan1(year.div_euclid(10) * 10),
        "year" => jan1(year),
        "weekYear" => calendar::from_iso_ywd(calendar::iso_week(days).0, 1, 1),
        "quarter" => calendar::ymd_to_days(year, ((month - 1) / 3) * 3 + 1, 1),
        "month" => calendar::ymd_to_days(year, month, 1),
        "week" => Some(days - i64::from(calendar::num_days_from_monday(days))),
        "day" => Some(days),
        _ => None,
    }
}

/// Truncate a time-of-day (nanoseconds since midnight) to the start of a unit
/// (openCypher `localtime.truncate(unit, …)` and the time component of the other
/// `*.truncate`): `hour`/`minute`/`second`/`millisecond`/`microsecond` floor to
/// that boundary; any unit coarser than a time-of-day (`day` and up) → midnight
/// (`0`). `None` for an unknown unit. (#920)
#[must_use]
pub fn truncate_time_nanos(nanos: i64, unit: &str) -> Option<i64> {
    let floor = |step: i64| (nanos / step) * step;
    match unit {
        "hour" => Some(floor(3_600_000_000_000)),
        "minute" => Some(floor(60_000_000_000)),
        "second" => Some(floor(1_000_000_000)),
        "millisecond" => Some(floor(1_000_000)),
        "microsecond" => Some(floor(1_000)),
        "millennium" | "century" | "decade" | "year" | "weekYear" | "quarter" | "month"
        | "week" | "day" => Some(0),
        _ => None,
    }
}

/// The 1-based day-of-quarter of a date (Jan 1 / Apr 1 / Jul 1 / Oct 1 → 1).
/// Range-complete via `calendar` (the quarter-start month 1/4/7/10 is always
/// valid, so the fallback is never taken). (#1011)
fn base_day_of_quarter(days: i64) -> i64 {
    let (year, month, _) = crate::calendar::civil_from_days(days);
    let start_month = ((month - 1) / 3) * 3 + 1;
    let start = crate::calendar::ymd_to_days(year, start_month, 1).unwrap_or(days);
    days - start + 1
}

/// Build a date from calendar / ISO-week / ordinal / quarter fields, or from a
/// `date` anchor with a `week` override (the `Temporal1` anchored forms).
fn resolve_date(f: &Fields) -> Option<i64> {
    // Range-complete via `calendar` (#1011); the anchor is a nested date() (i64
    // days). `from_iso_ywd` validates `week`/`dayOfWeek` like the old chrono path.
    use crate::calendar;
    if let Some(anchor) = f_date(f, "date") {
        if f.contains_key("week") {
            let iso_year = match f_int(f, "year") {
                Some(y) => y,
                None => calendar::iso_week(anchor).0,
            };
            let week = u32::try_from(f_int(f, "week")?).ok()?;
            let dow = match f_int(f, "dayOfWeek") {
                Some(d) => u32::try_from(d).ok()?,
                None => calendar::iso_weekday(anchor),
            };
            return calendar::from_iso_ywd(iso_year, week, dow);
        }
        return Some(anchor);
    }

    let year = f_int(f, "year")?;
    if f.contains_key("month") || f.contains_key("day") {
        let month = u32::try_from(f_int(f, "month").unwrap_or(1)).ok()?;
        let day = u32::try_from(f_int(f, "day").unwrap_or(1)).ok()?;
        calendar::ymd_to_days(year, month, day)
    } else if f.contains_key("week") {
        let week = u32::try_from(f_int(f, "week")?).ok()?;
        let dow = u32::try_from(f_int(f, "dayOfWeek").unwrap_or(1)).ok()?;
        calendar::from_iso_ywd(year, week, dow)
    } else if f.contains_key("ordinalDay") {
        calendar::from_ordinal(year, u32::try_from(f_int(f, "ordinalDay")?).ok()?)
    } else if f.contains_key("quarter") {
        let quarter = f_int(f, "quarter")?;
        let day_of_quarter = f_int(f, "dayOfQuarter").unwrap_or(1);
        let start_month = u32::try_from((quarter - 1) * 3 + 1).ok()?;
        let start = calendar::ymd_to_days(year, start_month, 1)?;
        let offset = i64::try_from(u64::try_from(day_of_quarter - 1).ok()?).ok()?;
        Some(start + offset)
    } else {
        calendar::ymd_to_days(year, 1, 1)
    }
}

/// Build a time of day from `hour`/`minute`/`second` plus additive subsecond
/// fields (`millisecond` + `microsecond` + `nanosecond`). Absent components
/// default to zero; the rendered precision follows the finest field present.
fn resolve_time(f: &Fields) -> Option<TimeParts> {
    let hour = u32::try_from(f_int(f, "hour").unwrap_or(0)).ok()?;
    let minute = u32::try_from(f_int(f, "minute").unwrap_or(0)).ok()?;
    let second = u32::try_from(f_int(f, "second").unwrap_or(0)).ok()?;
    let milli = f_int(f, "millisecond").unwrap_or(0);
    let micro = f_int(f, "microsecond").unwrap_or(0);
    let nano = f_int(f, "nanosecond").unwrap_or(0);
    let nanos = u32::try_from(milli * 1_000_000 + micro * 1_000 + nano).ok()?;

    let precision = if f.contains_key("millisecond")
        || f.contains_key("microsecond")
        || f.contains_key("nanosecond")
    {
        TimePrecision::SubSecond
    } else if f.contains_key("second") {
        TimePrecision::Second
    } else {
        TimePrecision::Minute
    };

    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?;
    Some(TimeParts {
        hour,
        minute,
        second,
        nanos,
        precision,
    })
}

/// Resolve a map's `timezone` field to an offset and optional named-zone label.
/// Absent → UTC (`Z`). An offset string is parsed directly; a named zone is
/// resolved against `date` (required) at that instant.
fn resolve_timezone(
    f: &Fields,
    date: Option<i64>,
    time: &TimeParts,
) -> Option<(Offset, Option<String>)> {
    match f_str(f, "timezone") {
        None => Some((
            Offset {
                seconds: 0,
                has_seconds: false,
            },
            None,
        )),
        Some(tz) if tz == "Z" || tz.starts_with('+') || tz.starts_with('-') => {
            Some((parse_offset(tz)?, None))
        }
        Some(tz) => Some((resolve_zone_offset(date?, time, tz)?, Some(tz.to_string()))),
    }
}

/// `time({…})` — a time of day with a zone offset (default `Z`).
fn build_time_map(f: &Fields) -> Option<String> {
    if !f.contains_key("hour") {
        return None;
    }
    let time = resolve_time(f)?;
    let (offset, _) = resolve_timezone(f, None, &time)?;
    Some(format!("{}{}", format_time(&time), format_offset(&offset)))
}

/// `datetime({…})` — a date and time with a zone (default `Z`, offset, or named).
fn build_date_time_map(f: &Fields) -> Option<String> {
    use std::fmt::Write as _;
    let date = resolve_date(f)?;
    let time = resolve_time(f)?;
    let (offset, zone) = resolve_timezone(f, Some(date), &time)?;
    let mut out = format!(
        "{}T{}{}",
        format_date(date),
        format_time(&time),
        format_offset(&offset)
    );
    if let Some(z) = zone {
        write!(out, "[{z}]").unwrap();
    }
    Some(out)
}

/// The Gregorian average month length in days (`MONTH_SECS / DAY_SECS =
/// 30.436875`), the openCypher constant for carrying a fractional month into
/// days (CIP2015-08-06). (#920)
const AVG_DAYS_PER_MONTH: f64 = MONTH_SECS / DAY_SECS;

/// openCypher "approximate" normalisation: carry a fractional month into days
/// (× [`AVG_DAYS_PER_MONTH`]) and a fractional day into the sub-day seconds,
/// truncating each level toward zero so whole months/days land in their own
/// fields and only the genuine sub-day remainder stays in `seconds`. (#920)
#[allow(
    clippy::cast_possible_truncation,
    reason = "duration component magnitudes stay within i64 for the corpus"
)]
fn approximate_duration(months_f: f64, days_f: f64, seconds_f: f64) -> DurationValue {
    let months = months_f.trunc();
    let days_f = days_f + (months_f - months) * AVG_DAYS_PER_MONTH;
    let days = days_f.trunc();
    let seconds_total = seconds_f + (days_f - days) * DAY_SECS;
    // FLOOR-split the sub-day seconds into whole seconds + a NON-NEGATIVE
    // nanos-of-second (the canonical form). Construction-scale values are exactly
    // representable in f64, so this is lossless here.
    let mut whole_secs = seconds_total.floor() as i64;
    let mut sub_nanos = ((seconds_total - seconds_total.floor()) * 1e9).round() as i64;
    // `.round()` of a fraction ≥ 0.9999999995 yields exactly 1e9; carry it into
    // seconds so `nanos` stays in `[0, 1e9)` (#1011).
    if sub_nanos >= 1_000_000_000 {
        whole_secs += 1;
        sub_nanos -= 1_000_000_000;
    }
    DurationValue {
        months: months as i64,
        days: days as i64,
        seconds: whole_secs,
        nanos: sub_nanos,
    }
}

/// `duration({years, months, weeks, days, hours, minutes, seconds, …})`.
/// Fractional larger units carry into smaller ones via [`approximate_duration`]
/// (a fractional month → days, a fractional day → sub-day time), so whole days
/// live in the `days` field and rendering needs no day-fold.
fn build_duration_map(f: &Fields) -> Option<DurationValue> {
    let (mut months_f, mut days_f, mut seconds_f) = (0.0, 0.0, 0.0);
    let mut any = false;
    for (key, factor_secs, into) in [
        ("years", YEAR_SECS, Unit::Month(12)),
        ("months", MONTH_SECS, Unit::Month(1)),
        ("weeks", DAY_SECS * 7.0, Unit::Day(7)),
        ("days", DAY_SECS, Unit::Day(1)),
        ("hours", 3600.0, Unit::Sec),
        ("minutes", 60.0, Unit::Sec),
        ("seconds", 1.0, Unit::Sec),
        ("milliseconds", 1e-3, Unit::Sec),
        ("microseconds", 1e-6, Unit::Sec),
        ("nanoseconds", 1e-9, Unit::Sec),
    ] {
        let Some(val) = f_num(f, key) else { continue };
        any = true;
        match into {
            #[allow(clippy::cast_precision_loss, reason = "mult is 1 or 12")]
            Unit::Month(mult) => months_f += val * mult as f64,
            Unit::Day(mult) => days_f += val * f64::from(mult),
            Unit::Sec => seconds_f += val * factor_secs,
        }
    }
    any.then(|| approximate_duration(months_f, days_f, seconds_f))
}

/// How a duration map field folds into the (months, days, seconds) model.
enum Unit {
    Month(i64),
    Day(i32),
    Sec,
}

/// `datetime.fromepoch(seconds, nanoseconds)` — a UTC datetime from a Unix
/// epoch offset.
#[must_use]
pub fn render_from_epoch(seconds: i64, nanos: i64) -> Option<String> {
    let dt = chrono::DateTime::from_timestamp(seconds, u32::try_from(nanos).ok()?)?;
    Some(format_epoch_datetime(dt.naive_utc()))
}

/// `datetime.fromepochmillis(milliseconds)` — a UTC datetime from Unix epoch
/// milliseconds.
#[must_use]
pub fn render_from_epoch_millis(millis: i64) -> Option<String> {
    let dt = chrono::DateTime::from_timestamp_millis(millis)?;
    Some(format_epoch_datetime(dt.naive_utc()))
}

/// Render an epoch-derived UTC datetime as `YYYY-MM-DDTHH:MM:SS[.fff]Z`.
fn format_epoch_datetime(naive: chrono::NaiveDateTime) -> String {
    let nanos = naive.and_utc().timestamp_subsec_nanos();
    let time = TimeParts {
        hour: naive.hour(),
        minute: naive.minute(),
        second: naive.second(),
        nanos,
        precision: if nanos > 0 {
            TimePrecision::SubSecond
        } else {
            TimePrecision::Second
        },
    };
    // An epoch-derived datetime is always in chrono's range, so the days bridge
    // never returns `None` here.
    let days = date_to_epoch_days(naive.date()).unwrap_or(0);
    format!("{}T{}Z", format_date(days), format_time(&time))
}

/// Canonical openCypher rendering of `date(<string>)`, or `None` if the string
/// is not a recognised ISO date form.
#[must_use]
pub fn render_date(s: &str) -> Option<String> {
    Some(format_date(parse_date_string(s.trim())?))
}

/// Canonical openCypher rendering of `localtime(<string>)` — a time of day with
/// no zone — or `None`.
#[must_use]
pub fn render_local_time(s: &str) -> Option<String> {
    let s = s.trim();
    // A local time carries no offset; reject one rather than silently dropping it.
    let (_, offset) = split_time_offset(s);
    if offset.is_some() {
        return None;
    }
    Some(format_time(&parse_time_of_day(s)?))
}

// ---------------------------------------------------------------------------
// localtime typed value (Arrow Time64(Nanosecond), ADR 0009)
// ---------------------------------------------------------------------------

/// Nanoseconds-since-midnight for a [`TimeParts`] (its render precision is
/// irrelevant to the numeric value).
fn nanos_of_day(t: &TimeParts) -> i64 {
    (i64::from(t.hour) * 3600 + i64::from(t.minute) * 60 + i64::from(t.second)) * 1_000_000_000
        + i64::from(t.nanos)
}

/// The [`TimeParts`] for a `Time64(Nanosecond)` localtime, with render precision
/// derived from the value (trailing-zero trim — ADR 0009): sub-second if any
/// fraction, else second if non-zero seconds, else minute.
fn time_parts_from_nanos(nanos: i64) -> TimeParts {
    let nanos = nanos.rem_euclid(86_400_000_000_000);
    let secs = nanos / 1_000_000_000;
    let sub = u32::try_from(nanos % 1_000_000_000).unwrap_or(0);
    let precision = if sub != 0 {
        TimePrecision::SubSecond
    } else if secs % 60 != 0 {
        TimePrecision::Second
    } else {
        TimePrecision::Minute
    };
    TimeParts {
        hour: u32::try_from(secs / 3600).unwrap_or(0),
        minute: u32::try_from((secs % 3600) / 60).unwrap_or(0),
        second: u32::try_from(secs % 60).unwrap_or(0),
        nanos: sub,
        precision,
    }
}

/// Build a `localtime` (nanoseconds-of-day) from a literal field map. (ADR 0009)
#[must_use]
pub fn localtime_nanos_from_map(fields: &Fields) -> Option<i64> {
    resolve_time(fields).as_ref().map(nanos_of_day)
}

/// Parse a strict `localtime(<string>)` to nanoseconds-of-day — a time of day
/// with NO offset (an offset is rejected, matching [`render_local_time`]).
#[must_use]
pub fn localtime_nanos_from_str(s: &str) -> Option<i64> {
    let s = s.trim();
    let (_, offset) = split_time_offset(s);
    if offset.is_some() {
        return None;
    }
    Some(nanos_of_day(&parse_time_of_day(s)?))
}

/// Extract the time-of-day (nanoseconds-of-day) from ANY temporal string —
/// `localtime`, `time` (offset dropped), or `localdatetime`/`datetime` (date
/// prefix and any zone dropped) — for projecting a localtime out of another
/// value (`localtime({time: other})`). (ADR 0009)
#[must_use]
pub fn time_of_day_nanos_any(s: &str) -> Option<i64> {
    let s = s.trim();
    let s = s.split_once('[').map_or(s, |(head, _)| head); // drop `[Zone]`
    let s = s.split_once('T').map_or(s, |(_, time)| time); // drop `YYYY-…T` date prefix
    let (time, _offset) = split_time_offset(s); // drop trailing offset
    Some(nanos_of_day(&parse_time_of_day(time)?))
}

/// Canonical openCypher rendering of an Arrow `Time64(Nanosecond)` localtime
/// (`HH:MM` / `HH:MM:SS` / `HH:MM:SS.fff…`, trailing-zero subseconds trimmed).
#[must_use]
pub fn render_localtime_nanos(nanos: i64) -> String {
    format_time(&time_parts_from_nanos(nanos))
}

/// Component overrides for `localtime` projection (`localtime({time: base, …})`):
/// a field left `None` keeps the base's value; the sub-second fields, if any are
/// present, jointly replace the base fraction.
#[derive(Default)]
pub struct LocalTimeOverrides {
    /// Hour of day (0–23).
    pub hour: Option<i64>,
    /// Minute (0–59).
    pub minute: Option<i64>,
    /// Second (0–59).
    pub second: Option<i64>,
    /// Milliseconds of the second (additive with micro/nano).
    pub millisecond: Option<i64>,
    /// Microseconds of the second (additive).
    pub microsecond: Option<i64>,
    /// Nanoseconds of the second (additive).
    pub nanosecond: Option<i64>,
}

/// Project a base localtime through component overrides (select-semantics:
/// replace the named components, keep the rest). `None` for an out-of-range
/// component. (ADR 0009)
#[must_use]
pub fn project_localtime(base_nanos: i64, o: &LocalTimeOverrides) -> Option<i64> {
    let base = time_parts_from_nanos(base_nanos);
    let hour = match o.hour {
        Some(h) => u32::try_from(h).ok()?,
        None => base.hour,
    };
    let minute = match o.minute {
        Some(m) => u32::try_from(m).ok()?,
        None => base.minute,
    };
    let second = match o.second {
        Some(s) => u32::try_from(s).ok()?,
        None => base.second,
    };
    let nanos = if o.millisecond.is_some() || o.microsecond.is_some() || o.nanosecond.is_some() {
        // The sub-second is three additive 3-digit groups (ms·1e6 + µs·1e3 + ns).
        // Select-semantics: replace only the named groups, KEEP the others from
        // the base — so `truncate('millisecond', …) {nanosecond: 2}` keeps the
        // base's millisecond and yields `.645000002`, not `.000000002` (#920).
        let base_milli = i64::from(base.nanos) / 1_000_000;
        let base_micro = (i64::from(base.nanos) / 1_000) % 1_000;
        let base_nano = i64::from(base.nanos) % 1_000;
        let sub = o.millisecond.unwrap_or(base_milli) * 1_000_000
            + o.microsecond.unwrap_or(base_micro) * 1_000
            + o.nanosecond.unwrap_or(base_nano);
        u32::try_from(sub).ok()?
    } else {
        base.nanos
    };
    let parts = TimeParts {
        hour,
        minute,
        second,
        nanos,
        precision: TimePrecision::Minute, // unused by `nanos_of_day`
    };
    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?; // validate
    Some(nanos_of_day(&parts))
}

// ---------------------------------------------------------------------------
// localdatetime typed value (Arrow Struct{date: Date32, time: Time64(ns)}, ADR
// 0009). A two-field value (not a single nanosecond timestamp) so it spans the
// full openCypher year range (1…9999+) at nanosecond precision — an `i64`
// epoch-nanosecond representation overflows around year 2262. The `date`-first
// field order makes DataFusion's row-format sort chronological.
// ---------------------------------------------------------------------------

/// Build a `localdatetime` (date + nanoseconds-of-day) from a literal field
/// map. (ADR 0009)
#[must_use]
pub fn localdatetime_parts_from_map(fields: &Fields) -> Option<(i64, i64)> {
    let date = resolve_date(fields)?;
    let time = resolve_time(fields)?;
    Some((date, nanos_of_day(&time)))
}

/// Parse a `localdatetime(<string>)` (`YYYY-…T HH:MM…`) to (date-days, nanoseconds-
/// of-day) — a date and time with NO offset (an offset is rejected, matching
/// [`render_local_date_time`]).
#[must_use]
pub fn localdatetime_parts_from_str(s: &str) -> Option<(i64, i64)> {
    let s = s.trim();
    // A date-only string is midnight (`localdatetime('2015-07-21')` →
    // `2015-07-21T00:00`, Temporal10 [10]); otherwise split date and time on `T`.
    let Some((date_str, time_str)) = s.split_once('T') else {
        return Some((parse_date_string(s)?, 0));
    };
    let date = parse_date_string(date_str)?;
    let (_, offset) = split_time_offset(time_str);
    if offset.is_some() {
        return None;
    }
    Some((date, nanos_of_day(&parse_time_of_day(time_str)?)))
}

/// Canonical openCypher rendering of a `localdatetime` (date-days + nanoseconds-
/// of-day): `YYYY-MM-DDTHH:MM[:SS[.fff…]]`, time precision derived from the value.
#[must_use]
pub fn render_localdatetime(date: i64, nanos_of_day: i64) -> String {
    format!(
        "{}T{}",
        format_date(date),
        render_localtime_nanos(nanos_of_day)
    )
}

/// Canonical openCypher rendering of `time(<string>)` — a time of day with a
/// zone offset — or `None`. The offset is required.
#[must_use]
pub fn render_time(s: &str) -> Option<String> {
    let (time, offset) = split_time_offset(s.trim());
    let time = parse_time_of_day(time)?;
    let offset = parse_offset(offset?)?;
    Some(format!("{}{}", format_time(&time), format_offset(&offset)))
}

// ---------------------------------------------------------------------------
// time typed value (Arrow Struct{time: Time64(ns), offset: Int32 seconds}, ADR
// 0009). A time of day WITH a zone offset. Stored as nanoseconds-of-day plus the
// offset in seconds (always whole minutes for `time()`).
// ---------------------------------------------------------------------------

/// Build a `time` (nanoseconds-of-day, offset-seconds) from a literal field map
/// (`time({hour, …, timezone})`; default zone is UTC). (ADR 0009)
#[must_use]
pub fn time_value_from_map(fields: &Fields) -> Option<(i64, i32)> {
    if !fields.contains_key("hour") {
        return None;
    }
    let time = resolve_time(fields)?;
    let (offset, _) = resolve_timezone(fields, None, &time)?;
    Some((nanos_of_day(&time), offset.seconds))
}

/// Parse a `time(<string>)` to (nanoseconds-of-day, offset-seconds). The offset
/// is REQUIRED (a `time` carries a zone). (ADR 0009)
#[must_use]
pub fn time_value_from_str(s: &str) -> Option<(i64, i32)> {
    let (time, offset) = split_time_offset(s.trim());
    // A bare `time('14:30')` (no offset) defaults to UTC (`Z`, offset 0) — the
    // openCypher default zone; an explicit offset is parsed as given. (#920)
    let offset_secs = match offset {
        Some(o) => parse_offset(o)?.seconds,
        None => 0,
    };
    Some((nanos_of_day(&parse_time_of_day(time)?), offset_secs))
}

/// Extract a time-of-day and (optional) offset from ANY temporal string for
/// `time` projection: `localtime`/`localdatetime` → offset `None` (the new zone
/// is *attached*); `time`/`datetime` → `Some(offset)` (a new zone *shifts* the
/// instant). Drops a named-zone suffix and a date prefix. (ADR 0009)
#[must_use]
pub fn time_of_day_with_offset(s: &str) -> Option<(i64, Option<i32>)> {
    let s = s.trim();
    let s = s.split_once('[').map_or(s, |(head, _)| head); // drop `[Zone]`
    let s = s.split_once('T').map_or(s, |(_, t)| t); // drop date prefix
    let (time, offset) = split_time_offset(s);
    let nanos = nanos_of_day(&parse_time_of_day(time)?);
    let offset = match offset {
        Some(o) => Some(parse_offset(o)?.seconds),
        None => None,
    };
    Some((nanos, offset))
}

/// Apply a `time` projection's zone semantics. With a new offset: if the base
/// already had one (`time`/`datetime`), the wall-clock time SHIFTS to preserve
/// the instant; otherwise (`localtime`/`localdatetime`) the offset is simply
/// ATTACHED. Without a new offset, the base offset (or UTC) is kept. (ADR 0009)
#[must_use]
pub fn project_time(
    base_nanos: i64,
    base_offset: Option<i32>,
    new_offset: Option<i32>,
) -> (i64, i32) {
    match (new_offset, base_offset) {
        (Some(new), Some(old)) => {
            let shifted =
                (base_nanos + i64::from(new - old) * 1_000_000_000).rem_euclid(86_400_000_000_000);
            (shifted, new)
        }
        (Some(new), None) => (base_nanos, new),
        (None, Some(old)) => (base_nanos, old),
        (None, None) => (base_nanos, 0),
    }
}

/// Canonical openCypher rendering of a `time` value (nanoseconds-of-day +
/// offset-seconds): time-of-day + offset (`Z` for UTC). (ADR 0009)
#[must_use]
pub fn render_time_value(nanos: i64, offset_seconds: i32) -> String {
    let offset = Offset {
        seconds: offset_seconds,
        has_seconds: offset_seconds % 60 != 0,
    };
    format!(
        "{}{}",
        render_localtime_nanos(nanos),
        format_offset(&offset)
    )
}

// ---------------------------------------------------------------------------
// datetime typed value (Arrow Struct{date: Date32, time: Time64(ns), offset:
// Int32, zone: Utf8?}, ADR 0009). A date + time-of-day + zone, where the zone
// is the resolved numeric offset plus an optional named-IANA-zone label (which
// renders as the trailing `[Zone]`).
// ---------------------------------------------------------------------------

/// The four-tuple representation of a `datetime`: date-days, nanoseconds-of-day,
/// resolved offset-seconds, and optional named-zone label.
type DateTimeParts = (i64, i64, i32, Option<String>);

/// Build a `datetime` from a literal field map (`datetime({…, timezone})`).
#[must_use]
pub fn datetime_value_from_map(f: &Fields) -> Option<DateTimeParts> {
    let date = resolve_date(f)?;
    let time = resolve_time(f)?;
    let (offset, zone) = resolve_timezone(f, Some(date), &time)?;
    Some((date, nanos_of_day(&time), offset.seconds, zone))
}

/// Parse a `datetime(<string>)` — offset form (`…T…+01:00`) or named-zone form
/// (`…T…[Europe/London]`, offset resolved at that instant). (ADR 0009)
#[must_use]
pub fn datetime_value_from_str(s: &str) -> Option<DateTimeParts> {
    let s = s.trim();
    if let Some(bracket) = s.find('[') {
        let zone = s.get(bracket + 1..)?.strip_suffix(']')?;
        let (date_str, time_offset) = s.get(..bracket)?.split_once('T')?;
        let date = parse_date_string(date_str)?;
        let (time_str, offset_str) = split_time_offset(time_offset);
        let time = parse_time_of_day(time_str)?;
        let offset = match offset_str {
            Some(o) => parse_offset(o)?,
            None => resolve_zone_offset(date, &time, zone)?,
        };
        return Some((
            date,
            nanos_of_day(&time),
            offset.seconds,
            Some(zone.to_string()),
        ));
    }
    let (date_str, time_offset) = s.split_once('T')?;
    let date = parse_date_string(date_str)?;
    let (time_str, offset_str) = split_time_offset(time_offset);
    let time = parse_time_of_day(time_str)?;
    Some((
        date,
        nanos_of_day(&time),
        parse_offset(offset_str?)?.seconds,
        None,
    ))
}

/// Extract `(nanoseconds-of-day, optional offset-seconds, optional zone label)`
/// from ANY temporal string — `localtime`/`localdatetime` → offset/zone `None`;
/// `time` → `Some(offset)`, no zone; `datetime` → offset + optional zone. Used
/// when projecting a `datetime` from another value. (ADR 0009)
#[must_use]
pub fn time_offset_zone(s: &str) -> Option<(i64, Option<i32>, Option<String>)> {
    let s = s.trim();
    let (head, zone) = match s.split_once('[') {
        Some((h, z)) => (h, z.strip_suffix(']').map(str::to_string)),
        None => (s, None),
    };
    let body = head.split_once('T').map_or(head, |(_, t)| t);
    let (time, offset) = split_time_offset(body);
    let nanos = nanos_of_day(&parse_time_of_day(time)?);
    let offset = match offset {
        Some(o) => Some(parse_offset(o)?.seconds),
        None => None,
    };
    Some((nanos, offset, zone))
}

/// Canonical openCypher rendering of a `datetime`: `YYYY-MM-DDTHH:MM…±HH:MM`
/// (`Z` for UTC), plus a trailing `[Zone]` for a named zone. (ADR 0009)
#[must_use]
pub fn render_datetime_value(
    date: i64,
    nanos: i64,
    offset_seconds: i32,
    zone: Option<&str>,
) -> String {
    use std::fmt::Write as _;
    let offset = Offset {
        seconds: offset_seconds,
        has_seconds: offset_seconds % 60 != 0,
    };
    let mut out = format!(
        "{}T{}{}",
        format_date(date),
        render_localtime_nanos(nanos),
        format_offset(&offset)
    );
    if let Some(z) = zone {
        write!(out, "[{z}]").unwrap();
    }
    out
}

/// Project a `datetime` (`Temporal3` [8]-[11]): apply the source's zone to the
/// local `(date, nanos)` after component overrides. With a new `timezone`: a
/// numeric offset or named zone SHIFTS the instant when the source already had
/// an offset (`time`/`datetime`), else ATTACHES (interpreting the local time as
/// being in that zone). Without one, the source offset/zone (or UTC) is kept.
/// (ADR 0009)
#[must_use]
pub fn project_datetime(
    date: i64,
    nanos: i64,
    src_offset: Option<i32>,
    src_zone: Option<&str>,
    new_tz: Option<&str>,
) -> Option<DateTimeParts> {
    use chrono::offset::{Offset as _, TimeZone};
    // Projection is never on the extreme-year path (Temporal3 operands are
    // ordinary dates), so the chrono bridge holds. (#1011)
    let base = epoch_days_to_date(date)?;
    let local = base.and_time(nanos_to_naive_time(nanos)?);
    // For a NAMED-zone source, `src_offset` was resolved at the source instant;
    // component overrides may have moved the local wall-clock onto a date with a
    // different DST offset, so re-resolve the source offset at the new local time
    // before applying zone semantics (#1008, Temporal3 [10]).
    let src_offset = match (src_offset, src_zone) {
        (Some(o), Some(z)) => Some(
            resolve_zone_offset(date, &time_parts_from_nanos(nanos), z)
                .map_or(o, |off| off.seconds),
        ),
        (o, _) => o,
    };
    let Some(tz) = new_tz else {
        // No new zone: keep the source's offset (and named zone), else UTC.
        return Some(match src_offset {
            Some(o) => (date, nanos, o, src_zone.map(str::to_string)),
            None => (date, nanos, 0, None),
        });
    };
    // A numeric offset designator?
    if let Some(new_off) = parse_offset_seconds(tz) {
        return Some(match src_offset {
            // Shift to preserve the instant.
            Some(src) => {
                let shifted = local + chrono::Duration::seconds(i64::from(new_off - src));
                (
                    date_to_epoch_days(shifted.date())?,
                    naive_time_nanos(shifted.time()),
                    new_off,
                    None,
                )
            }
            // Attach (the local time is already in the target offset).
            None => (date, nanos, new_off, None),
        });
    }
    // A named IANA zone.
    let zone: chrono_tz::Tz = tz.parse().ok()?;
    if let Some(src) = src_offset {
        // Shift: convert the instant to the named zone's local time.
        let utc = local - chrono::Duration::seconds(i64::from(src));
        let zoned = zone.from_utc_datetime(&utc);
        let off = zoned.offset().fix().local_minus_utc();
        let l = zoned.naive_local();
        Some((
            date_to_epoch_days(l.date())?,
            naive_time_nanos(l.time()),
            off,
            Some(tz.to_string()),
        ))
    } else {
        // Attach: interpret the local time as being in the named zone.
        let off = resolve_zone_offset(date, &time_parts_from_nanos(nanos), tz)?;
        Some((date, nanos, off.seconds, Some(tz.to_string())))
    }
}

/// A [`NaiveTime`] from nanoseconds-of-day.
fn nanos_to_naive_time(nanos: i64) -> Option<NaiveTime> {
    let tp = time_parts_from_nanos(nanos);
    NaiveTime::from_hms_nano_opt(tp.hour, tp.minute, tp.second, tp.nanos)
}

/// Nanoseconds-of-day for a [`NaiveTime`].
fn naive_time_nanos(t: NaiveTime) -> i64 {
    i64::from(t.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(t.nanosecond())
}

/// Canonical openCypher rendering of `localdatetime(<string>)` — a date and
/// time with no zone — or `None`.
#[must_use]
pub fn render_local_date_time(s: &str) -> Option<String> {
    let (date, time) = s.trim().split_once('T')?;
    let date = parse_date_string(date)?;
    let (_, offset) = split_time_offset(time);
    if offset.is_some() {
        return None;
    }
    let time = parse_time_of_day(time)?;
    Some(format!("{}T{}", format_date(date), format_time(&time)))
}

/// Canonical openCypher rendering of `datetime(<string>)` — a date and time
/// with a zone — or `None`. Handles both the offset form
/// (`…T21:40:32+01:00`) and the named-zone form (`…T21:40:32[Europe/London]`,
/// whose offset is resolved from the IANA tz database at that instant,
/// including historical LMT offsets like `+00:53:28`).
#[must_use]
pub fn render_date_time(s: &str) -> Option<String> {
    let s = s.trim();
    if let Some(bracket) = s.find('[') {
        return render_date_time_named(s, bracket);
    }
    let (date, time_offset) = s.split_once('T')?;
    let date = parse_date_string(date)?;
    let (time, offset) = split_time_offset(time_offset);
    let time = parse_time_of_day(time)?;
    let offset = parse_offset(offset?)?;
    Some(format!(
        "{}T{}{}",
        format_date(date),
        format_time(&time),
        format_offset(&offset)
    ))
}

/// Render the named-zone `datetime` form `…[Zone]`. An explicit offset before
/// the bracket is echoed (reformatted); otherwise the offset is resolved from
/// the named zone at that local datetime.
fn render_date_time_named(s: &str, bracket: usize) -> Option<String> {
    let zone = s.get(bracket + 1..)?.strip_suffix(']')?;
    let head = s.get(..bracket)?;
    let (date_str, time_offset) = head.split_once('T')?;
    let date = parse_date_string(date_str)?;
    let (time_str, offset_str) = split_time_offset(time_offset);
    let time = parse_time_of_day(time_str)?;

    let offset = match offset_str {
        Some(o) => parse_offset(o)?,
        None => resolve_zone_offset(date, &time, zone)?,
    };
    Some(format!(
        "{}T{}{}[{}]",
        format_date(date),
        format_time(&time),
        format_offset(&offset),
        zone
    ))
}

/// Resolve the UTC offset a named IANA zone was at on a given local datetime
/// (e.g. `Europe/London` in July → `+01:00`; `Europe/Stockholm` in 1818 →
/// the `+00:53:28` LMT). Returns `None` for an unknown zone or a local time
/// that doesn't exist in it.
fn resolve_zone_offset(days: i64, time: &TimeParts, zone: &str) -> Option<Offset> {
    use chrono::offset::{LocalResult, Offset as _, TimeZone};
    let tz: chrono_tz::Tz = zone.parse().ok()?;
    // chrono_tz needs a `NaiveDate`; a named zone only resolves for in-range years
    // (the corpus never pairs a named zone with an extreme year — those are
    // offset/unzoned). (#1011)
    let date = epoch_days_to_date(days)?;
    let naive = date.and_time(NaiveTime::from_hms_nano_opt(
        time.hour,
        time.minute,
        time.second,
        time.nanos,
    )?);
    let resolved = match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt,
        LocalResult::None => return None,
    };
    Some(Offset {
        seconds: resolved.offset().fix().local_minus_utc(),
        has_seconds: true,
    })
}

/// The finest unit named in a parsed time-of-day, which fixes how it renders:
/// always `HH:MM`, plus `:SS` for [`Second`](TimePrecision::Second) and
/// `.fff` for [`SubSecond`](TimePrecision::SubSecond). Two equal time *values*
/// can render differently (`21:40` vs `21:40:00`), so precision is tracked from
/// the input rather than derived from the value.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TimePrecision {
    Minute,
    Second,
    SubSecond,
}

/// A parsed time of day plus the precision at which to render it.
struct TimeParts {
    hour: u32,
    minute: u32,
    second: u32,
    nanos: u32,
    precision: TimePrecision,
}

/// A parsed UTC offset, in signed seconds, plus whether it carries a seconds
/// component (only the historical-LMT named-zone forms do).
struct Offset {
    seconds: i32,
    has_seconds: bool,
}

/// Parse a time of day in any ISO form the TCK uses — extended (`21:40:32.142`)
/// or basic (`214032.142`), down to hour-only (`21`). Returns `None` for an
/// out-of-range or malformed time.
fn parse_time_of_day(s: &str) -> Option<TimeParts> {
    let (main, frac) = match s.split_once('.') {
        Some((m, f)) => (m, Some(f)),
        None => (s, None),
    };

    let (hour, minute, second, base) = if main.contains(':') {
        let parts: Vec<&str> = main.split(':').collect();
        match parts.as_slice() {
            [h, m] => (h.parse().ok()?, m.parse().ok()?, 0, TimePrecision::Minute),
            [h, m, sec] => (
                h.parse().ok()?,
                m.parse().ok()?,
                sec.parse().ok()?,
                TimePrecision::Second,
            ),
            _ => return None,
        }
    } else {
        if !main.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        match main.len() {
            2 => (main.parse().ok()?, 0, 0, TimePrecision::Minute),
            4 => (
                main[..2].parse().ok()?,
                main[2..4].parse().ok()?,
                0,
                TimePrecision::Minute,
            ),
            6 => (
                main[..2].parse().ok()?,
                main[2..4].parse().ok()?,
                main[4..6].parse().ok()?,
                TimePrecision::Second,
            ),
            _ => return None,
        }
    };

    let (nanos, precision) = match frac {
        // Nanosecond is the finest representable precision; reject (rather than
        // silently truncate) a sub-nanosecond fraction of more than 9 digits.
        Some(f) if !f.is_empty() && f.len() <= 9 && f.bytes().all(|b| b.is_ascii_digit()) => {
            let mut digits = f.to_string();
            while digits.len() < 9 {
                digits.push('0');
            }
            (digits.parse().ok()?, TimePrecision::SubSecond)
        }
        Some(_) => return None,
        None => (0, base),
    };

    // Validate the (h, m, s, ns) tuple — rejects 25:00, 21:60, etc.
    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?;
    Some(TimeParts {
        hour,
        minute,
        second,
        nanos,
        precision,
    })
}

/// Render a [`TimeParts`] at its tracked precision (`HH:MM`, `HH:MM:SS`, or
/// `HH:MM:SS.fff` with trailing-zero subseconds trimmed).
fn format_time(t: &TimeParts) -> String {
    match t.precision {
        TimePrecision::Minute => format!("{:02}:{:02}", t.hour, t.minute),
        TimePrecision::Second => format!("{:02}:{:02}:{:02}", t.hour, t.minute, t.second),
        TimePrecision::SubSecond => {
            let frac = format!("{:09}", t.nanos);
            let frac = frac.trim_end_matches('0');
            if frac.is_empty() {
                format!("{:02}:{:02}:{:02}", t.hour, t.minute, t.second)
            } else {
                format!("{:02}:{:02}:{:02}.{frac}", t.hour, t.minute, t.second)
            }
        }
    }
}

/// Split a `time[offset]` string into the time-of-day and the (optional) offset
/// designator. The offset begins at the first `Z`, `+`, or `-` after position 0
/// — none of which occur within a local time of day.
fn split_time_offset(s: &str) -> (&str, Option<&str>) {
    for (i, c) in s.char_indices() {
        if i > 0 && matches!(c, 'Z' | '+' | '-') {
            return (&s[..i], Some(&s[i..]));
        }
    }
    (s, None)
}

/// Parse a UTC offset designator (`Z`, `±HH`, `±HH:MM`, …) to signed seconds —
/// the public entry point for a `time`/`datetime` `timezone` override. (ADR 0009)
#[must_use]
pub fn parse_offset_seconds(s: &str) -> Option<i32> {
    parse_offset(s).map(|o| o.seconds)
}

/// Parse a UTC offset designator: `Z`, `±HH`, `±HHMM`, `±HH:MM`, or `±HH:MM:SS`.
fn parse_offset(s: &str) -> Option<Offset> {
    if s == "Z" {
        return Some(Offset {
            seconds: 0,
            has_seconds: false,
        });
    }
    let (sign, rest) = match s.strip_prefix('+') {
        Some(rest) => (1, rest),
        None => (-1, s.strip_prefix('-')?),
    };
    let (hours, minutes, secs, has_seconds): (i32, i32, i32, bool) = if rest.contains(':') {
        let parts: Vec<&str> = rest.split(':').collect();
        // Each component must be unsigned digits — reject e.g. `+01:-30`.
        if !parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        {
            return None;
        }
        match parts.as_slice() {
            [h, m] => (h.parse().ok()?, m.parse().ok()?, 0, false),
            [h, m, sec] => (h.parse().ok()?, m.parse().ok()?, sec.parse().ok()?, true),
            _ => return None,
        }
    } else {
        if !rest.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        match rest.len() {
            2 => (rest.parse().ok()?, 0, 0, false),
            4 => (rest[..2].parse().ok()?, rest[2..4].parse().ok()?, 0, false),
            6 => (
                rest[..2].parse().ok()?,
                rest[2..4].parse().ok()?,
                rest[4..6].parse().ok()?,
                true,
            ),
            _ => return None,
        }
    };
    if hours >= 24 || minutes >= 60 || secs >= 60 {
        return None;
    }
    Some(Offset {
        seconds: sign * (hours * 3600 + minutes * 60 + secs),
        has_seconds,
    })
}

/// Render an [`Offset`]: a zero offset is `Z`; otherwise `±HH:MM` (plus `:SS`
/// for the historical second-bearing forms).
fn format_offset(o: &Offset) -> String {
    if o.seconds == 0 {
        return "Z".to_string();
    }
    let sign = if o.seconds < 0 { '-' } else { '+' };
    let abs = o.seconds.abs();
    let (hours, minutes, secs) = (abs / 3600, (abs % 3600) / 60, abs % 60);
    if o.has_seconds && secs != 0 {
        format!("{sign}{hours:02}:{minutes:02}:{secs:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}")
    }
}

/// Seconds in a day.
const DAY_SECS: f64 = 86_400.0;
/// Seconds in an average Gregorian month (`365.2425 / 12` days) — openCypher's
/// definition, used when a fractional month/year spills into the seconds field.
const MONTH_SECS: f64 = 2_629_746.0;
/// Seconds in an average Gregorian year (`12 * MONTH_SECS`).
const YEAR_SECS: f64 = 31_556_952.0;

/// A Cypher duration: months and days are kept distinct (a month is not a fixed
/// number of days), with everything finer than a day carried in `nanos`
/// (integer nanoseconds — exact even for very large sub-day spans, unlike an
/// `f64` seconds count whose mantissa drops sub-second precision past ~1e15ns).
/// A typed Cypher duration (ADR 0009 / #1011): signed `months` / `days` kept
/// distinct (a month is not a fixed number of days), and the sub-day time split
/// into whole `seconds` plus `nanos`-of-second. Splitting seconds from nanos lets
/// a billion-year `duration.inSeconds` (~6.3e16 s) fit `i64`, where a single
/// total-nanos field would overflow (~6.3e25 ns); `months: i64` likewise holds
/// the ~24e9-month spans `duration.between` can produce. `nanos` is in
/// `(-1e9, 1e9)` and shares the sign of `seconds` (truncating split).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurationValue {
    /// Signed whole months.
    pub months: i64,
    /// Signed whole days.
    pub days: i64,
    /// Signed whole sub-day seconds (carries the sub-day sign).
    pub seconds: i64,
    /// Nanoseconds-of-second, always `[0, 1e9)` (the Neo4j/openCypher canonical
    /// form — `seconds` carries the sign; `d.nanosecondsOfSecond` is non-negative).
    pub nanos: i64,
}

impl DurationValue {
    /// Build from `months`/`days` plus a total sub-day nanoseconds count,
    /// FLOOR-splitting it into `seconds` + non-negative `nanos`-of-second (the
    /// canonical form: `seconds` carries the sign, `nanos` is `[0, 1e9)`). Used
    /// where a sub-day span is already a bounded total-nanos value (construction,
    /// time-only `between`, native Arrow durations).
    #[must_use]
    pub fn from_total_nanos(months: i64, days: i64, total_nanos: i64) -> Self {
        Self {
            months,
            days,
            seconds: total_nanos.div_euclid(1_000_000_000),
            nanos: total_nanos.rem_euclid(1_000_000_000),
        }
    }
}

/// Canonical openCypher rendering of `duration(<string>)`, or `None` if the
/// string is not a recognised ISO-8601 duration. Handles the designator form
/// (`P14DT16H12M`, with decimal components like `P0.75M`/`P2.5W` spilling into
/// smaller units) and the alternative date-time form
/// (`P2012-02-02T14:37:21.545`).
#[must_use]
pub fn render_duration(s: &str) -> Option<String> {
    Some(format_duration(&parse_duration(s.trim())?))
}

/// Parse `duration(<string>)` to a typed [`DurationValue`]. (#920/#1011)
#[must_use]
pub fn duration_value_from_str(s: &str) -> Option<DurationValue> {
    parse_duration(s.trim())
}

/// Build a `duration({…})` [`DurationValue`] from a literal map. (#920/#1011)
#[must_use]
pub fn duration_value_from_map(fields: &Fields) -> Option<DurationValue> {
    build_duration_map(fields)
}

/// Canonical openCypher rendering of a typed [`DurationValue`] (reuses the same
/// designator formatter as the string path; the integer `seconds`/`nanos` are
/// preserved exactly, so a very large span renders without f64 precision loss). (#920)
#[must_use]
pub fn render_duration_value(dur: &DurationValue) -> String {
    format_duration(dur)
}

/// A duration component accessor (`d.years`, `d.monthsOfQuarter`,
/// `d.secondsOfMinute`, `d.nanosecondsOfSecond`, …) over a typed duration. The
/// `*Of*` forms give the component within the next-larger unit; the plain forms
/// give the total in that unit (truncated toward zero). `None` for an unknown
/// name. (#920)
#[must_use]
pub fn duration_component(dur: &DurationValue, name: &str) -> Option<i64> {
    let (months, days, secs) = (dur.months, dur.days, dur.seconds);
    let sub = dur.nanos; // nanoseconds-of-second, same sign as `secs`
    // Cumulative sub-second totals in i128, then narrow: a >~292-year `seconds`
    // (which `duration.inSeconds` over an extreme span produces) makes
    // `seconds * 1e9` overflow i64 — that would panic in debug and silently wrap
    // in release, re-introducing the very overflow the seconds/nanos split
    // avoids. For a span whose total-nanoseconds genuinely exceeds i64 the
    // accessor is unrepresentable, so return NULL rather than a wrong number.
    // (#1011)
    let total_nanos = || i128::from(secs) * 1_000_000_000 + i128::from(sub);
    let v = match name {
        "years" => months / 12,
        "quarters" => months / 3,
        "months" => months,
        "monthsOfYear" => months % 12,
        "monthsOfQuarter" => months % 3,
        "quartersOfYear" => (months / 3) % 4,
        "weeks" => days / 7,
        "days" => days,
        "daysOfWeek" => days % 7,
        "hours" => secs / 3600,
        "minutes" => secs / 60,
        "seconds" => secs,
        "minutesOfHour" => (secs / 60) % 60,
        "secondsOfMinute" => secs % 60,
        "milliseconds" => return i64::try_from(total_nanos() / 1_000_000).ok(),
        "microseconds" => return i64::try_from(total_nanos() / 1_000).ok(),
        "nanoseconds" => return i64::try_from(total_nanos()).ok(),
        "millisecondsOfSecond" => sub / 1_000_000,
        "microsecondsOfSecond" => sub / 1_000,
        "nanosecondsOfSecond" => sub,
        _ => return None,
    };
    Some(v)
}

/// Whether `name` is a duration component accessor (see [`duration_component`]).
#[must_use]
pub fn is_duration_accessor(name: &str) -> bool {
    duration_component(
        &DurationValue {
            months: 0,
            days: 0,
            seconds: 0,
            nanos: 0,
        },
        name,
    )
    .is_some()
}

/// Which `duration.between`-family function: the full split, or a single-unit total.
#[derive(Clone, Copy)]
pub enum BetweenMode {
    /// `duration.between` — months + days + nanos, calendar-aware.
    Between,
    /// `duration.inMonths` — whole months only.
    Months,
    /// `duration.inDays` — whole days only.
    Days,
    /// `duration.inSeconds` — total seconds (as nanos) only.
    Seconds,
}

/// A reduced temporal operand for [`duration_between`]: an optional date (`None`
/// for a time-only `localtime`/`time`), a time-of-day in nanoseconds, an optional
/// zone offset in seconds (`None` for an unzoned value), and an optional named
/// IANA zone (`Some` only for a `datetime` constructed with a zone name — needed
/// to re-resolve the offset across a DST transition). (#920/#1007)
pub type BetweenOperand = (Option<i64>, i64, Option<i32>, Option<String>);

const DAY_NANOS: i64 = 86_400_000_000_000;

/// A wall-clock instant for duration arithmetic: i64 days-since-epoch plus
/// nanoseconds-of-day in `[0, DAY_NANOS)`. Replaces `NaiveDateTime` so a
/// billion-year span (#1011) stays representable; being normalised, it orders
/// lexicographically as a tuple.
type Instant = (i64, i64);

/// Normalise `(days, nanos)` — where `nanos` may fall outside `[0, DAY_NANOS)`
/// after an offset shift — into a canonical [`Instant`], carrying the overflow
/// into days. (Replaces the old `datetime_from`.)
fn instant_from(days: i64, nanos: i64) -> Instant {
    (
        days + nanos.div_euclid(DAY_NANOS),
        nanos.rem_euclid(DAY_NANOS),
    )
}

/// Add a signed number of calendar months to an instant, day-clamped, keeping the
/// time-of-day. Uses [`crate::calendar`] (range-complete) rather than chrono's
/// unsigned `Months`. (#1011)
fn add_signed_months(dt: Instant, m: i64) -> Instant {
    (crate::calendar::add_months_to_days(dt.0, m), dt.1)
}

/// Whole days from `dt1` to `dt2`, truncated toward zero (chrono `num_days`
/// semantics), computed in i128 so a billion-year span never overflows. (#1011)
#[allow(
    clippy::cast_possible_truncation,
    reason = "the day quotient fits i64 across the full year range (±~7.3e11 days); \
              i128 only guards the nanosecond intermediate"
)]
fn instant_num_days(dt1: Instant, dt2: Instant) -> i64 {
    let total = i128::from(dt2.0 - dt1.0) * i128::from(DAY_NANOS) + i128::from(dt2.1 - dt1.1);
    (total / i128::from(DAY_NANOS)) as i64
}

/// The sub-span nanoseconds from `dt1` to `dt2`, for a span already known to be
/// small (the sub-month `between` remainder — always < ~1 month, so it fits i64).
fn instant_sub_nanos(dt1: Instant, dt2: Instant) -> i64 {
    (dt2.0 - dt1.0) * DAY_NANOS + (dt2.1 - dt1.1)
}

/// Elapsed whole `seconds` + non-negative sub-second `nanos` from `dt1` to `dt2`
/// as a sub-day-only [`DurationValue`] (`duration.inSeconds`). Seconds are formed
/// from the day span directly (× 86 400) so a billion-year span never builds a
/// total-nanos value that overflows i64 (#1011); the sub-day nanos difference is
/// FLOOR-split into the canonical (sign-on-seconds, non-negative nanos) form.
fn elapsed_seconds_between(dt1: Instant, dt2: Instant) -> DurationValue {
    let day_secs = (dt2.0 - dt1.0) * 86_400;
    let nanos_diff = dt2.1 - dt1.1; // in (-DAY_NANOS, DAY_NANOS)
    DurationValue {
        months: 0,
        days: 0,
        seconds: day_secs + nanos_diff.div_euclid(1_000_000_000),
        nanos: nanos_diff.rem_euclid(1_000_000_000),
    }
}

/// Resolve a [`BetweenOperand`] to a real UTC [`Instant`] for elapsed-seconds
/// maths (#1007). The local wall-clock is `op`'s own date (or, for a time-only
/// operand, the `partner`'s date). The UTC offset is `op`'s own when it carries
/// one, else — for an unzoned operand — the `partner`'s named zone resolved AT
/// that local time (DST-aware), or the partner's numeric offset, or `0` if
/// neither is zoned.
fn between_instant(op: &BetweenOperand, partner: &BetweenOperand) -> Option<Instant> {
    let (date, nanos, offset, _) = op;
    let (p_date, _, p_offset, p_zone) = partner;
    let date = date.or(*p_date)?;
    let off = match offset {
        Some(o) => *o,
        None => match p_zone.as_deref() {
            Some(z) => resolve_zone_offset(date, &time_parts_from_nanos(*nanos), z)?.seconds,
            None => p_offset.unwrap_or(0),
        },
    };
    Some(instant_from(date, *nanos - i64::from(off) * 1_000_000_000))
}

/// The whole calendar months from `dt1` to `dt2`, truncated toward zero: the
/// count closest to zero whose addition to `dt1` does not pass `dt2`.
///
/// The rounding direction follows the SPAN direction (`dt2` vs `dt1`), NOT the
/// sign of the raw calendar-month difference. They can disagree when the span is
/// under a month but crosses into an earlier day-of-month — e.g. from
/// `Jan 2 10:00` back to `Jan 1 12:00` the calendar diff is 0 yet the span is
/// negative; keying off `m >= 0` there wrongly decremented to -1 and spilled a
/// spurious `-1M30D` into `duration.between` (#920).
fn whole_months(dt1: Instant, dt2: Instant) -> i64 {
    let (y1, m1, _) = crate::calendar::civil_from_days(dt1.0);
    let (y2, m2, _) = crate::calendar::civil_from_days(dt2.0);
    let mut m = (y2 - y1) * 12 + (i64::from(m2) - i64::from(m1));
    let cand = add_signed_months(dt1, m);
    if dt2 >= dt1 {
        // Forward span: don't overshoot past dt2.
        if cand > dt2 {
            m -= 1;
        }
    } else if cand < dt2 {
        // Backward span: don't overshoot before dt2.
        m += 1;
    }
    m
}

/// Compute `duration.between`/`inMonths`/`inDays`/`inSeconds` from `a` to `b` as
/// a typed [`DurationValue`] (#920/#1011). Both operands dated → a calendar-aware
/// month/day/time split (shifted to UTC instants only when both carry a zone
/// offset); either operand time-only → just the time-of-day difference (no
/// month/day span), offset-adjusted only when both are zoned.
#[allow(
    clippy::single_match_else,
    reason = "the both-dated arm is the substantive calendar path; the time-only \
              else is the fallthrough — a match reads clearer than nested if-let"
)]
#[must_use]
pub fn duration_between(
    a: &BetweenOperand,
    b: &BetweenOperand,
    mode: BetweenMode,
) -> Option<DurationValue> {
    let (d1, n1, o1, _) = a;
    let (d2, n2, o2, _) = b;
    let (d1, n1, o1) = (*d1, *n1, *o1);
    let (d2, n2, o2) = (*d2, *n2, *o2);
    let both_off = o1.zip(o2);
    let shift = |n: i64, o: i32| n - i64::from(o) * 1_000_000_000;
    let zero = DurationValue {
        months: 0,
        days: 0,
        seconds: 0,
        nanos: 0,
    };
    // Elapsed SECONDS with at least one dated operand: resolve both to real UTC
    // instants so a named-zone DST transition is honoured (the day Stockholm
    // falls back has 25 wall-clock hours). An unzoned operand is interpreted in
    // the other's named zone; a time-only one borrows the dated one's date.
    // (#1007, Temporal10 [8]) Computed via `num_seconds` + `subsec_nanos` so a
    // billion-year span fits `i64` (#1011, Temporal10 [10]). Calendar modes keep
    // the wall-clock path below.
    if matches!(mode, BetweenMode::Seconds) && (d1.is_some() || d2.is_some()) {
        let dt1 = between_instant(a, b)?;
        let dt2 = between_instant(b, a)?;
        return Some(elapsed_seconds_between(dt1, dt2));
    }
    match (d1, d2) {
        (Some(da), Some(db)) => {
            let (dt1, dt2) = if let Some((oa, ob)) = both_off {
                (
                    instant_from(da, shift(n1, oa)),
                    instant_from(db, shift(n2, ob)),
                )
            } else {
                (instant_from(da, n1), instant_from(db, n2))
            };
            match mode {
                // i64 months/days — no narrowing, so billion-year spans survive
                // (#1011, Temporal10 [9]).
                BetweenMode::Months => Some(DurationValue {
                    months: whole_months(dt1, dt2),
                    ..zero
                }),
                BetweenMode::Days => Some(DurationValue {
                    days: instant_num_days(dt1, dt2),
                    ..zero
                }),
                BetweenMode::Seconds => Some(elapsed_seconds_between(dt1, dt2)),
                BetweenMode::Between => {
                    let m = whole_months(dt1, dt2);
                    // The sub-month remainder is always < ~1 month, so its total
                    // nanoseconds fit i64; split days then seconds/nanos,
                    // truncating toward zero so every field shares the sign.
                    let rem = instant_sub_nanos(add_signed_months(dt1, m), dt2);
                    Some(DurationValue::from_total_nanos(
                        m,
                        rem / DAY_NANOS,
                        rem % DAY_NANOS,
                    ))
                }
            }
        }
        // At least one operand is time-only: no month/day span, just the bounded
        // sub-day time-of-day difference.
        _ => {
            let diff = match (mode, both_off) {
                (BetweenMode::Months | BetweenMode::Days, _) => 0,
                (_, Some((oa, ob))) => shift(n2, ob) - shift(n1, oa),
                (_, None) => n2 - n1,
            };
            Some(DurationValue::from_total_nanos(0, 0, diff))
        }
    }
}

/// `duration * factor` / `duration / factor` (#920 Temporal8 [7]). Scales each
/// component by `factor`, then normalises with the openCypher "approximate"
/// rule: a fractional month overflows into days (× the Gregorian average month
/// length, `MONTH_SECS / DAY_SECS = 30.436875` days), a fractional day overflows
/// into the sub-day time, each level truncated toward zero. `factor` is the
/// multiplier (`* n`) or `1/n` is applied by the caller for division — here we
/// take the already-resolved factor as `num` with `divide` selecting `1/num`
/// component-wise to preserve precision.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "scaling is inherently f64; the sub-day total is computed in f64 \
              (not i64 seconds*1e9) to avoid an i64 overflow for large durations, \
              and corpus durations are small whole counts"
)]
pub fn scale_duration(dur: &DurationValue, num: f64, divide: bool) -> DurationValue {
    const DAY_NANOS_F: f64 = DAY_SECS * 1e9;
    let op = |x: f64| if divide { x / num } else { x * num };
    let m = op(dur.months as f64);
    let d = op(dur.days as f64);
    let n = op(dur.seconds as f64 * 1e9 + dur.nanos as f64);

    let m_whole = m.trunc();
    let d_total = d + (m - m_whole) * AVG_DAYS_PER_MONTH;
    let d_whole = d_total.trunc();
    let n_total = n + (d_total - d_whole) * DAY_NANOS_F;

    #[allow(
        clippy::cast_possible_truncation,
        reason = "scaled component magnitudes stay within i64 for the corpus"
    )]
    DurationValue::from_total_nanos(m_whole as i64, d_whole as i64, n_total.trunc() as i64)
}

/// `date + duration` (#920): only date-precision components apply — add the
/// signed months then days. The duration's sub-day time is **ignored** (a date
/// has no time-of-day; openCypher does not carry it into days). Returns the date
/// (i64 days), range-complete via [`crate::calendar`] (#1011).
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    reason = "the whole-day quotient fits i64; i128 only guards the nanosecond product"
)]
pub fn date_plus_duration(date: i64, dur: &DurationValue) -> i64 {
    let after_months = crate::calendar::add_months_to_days(date, dur.months);
    // A date has no time-of-day, but the WHOLE days in the duration's sub-day
    // time still advance the date (the sub-day remainder is dropped). Integer
    // division truncates toward zero (i128 so a large-second duration can't
    // overflow), so a negated (subtract) duration carries the matching whole day
    // in the negative direction (#920 Temporal8 [1]).
    let total_nanos = i128::from(dur.seconds) * 1_000_000_000 + i128::from(dur.nanos);
    let extra_days = (total_nanos / i128::from(DAY_NANOS)) as i64;
    after_months + dur.days + extra_days
}

/// `localtime/time + duration` (#920): only the sub-day time applies (months/days
/// are irrelevant to a time-of-day), wrapping mod 24h.
#[must_use]
pub fn localtime_plus_duration(nanos_of_day: i64, dur_nanos: i64) -> i64 {
    (nanos_of_day + dur_nanos).rem_euclid(DAY_NANOS)
}

/// `localdatetime/datetime + duration` (#920): add the signed months, then days,
/// then the sub-day time (carrying whole-day overflow into the date). Returns the
/// resulting `(date, nanos_of_day)`.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    reason = "the day carry and nanos-of-day both fit i64; i128 only guards the \
              nanosecond product"
)]
pub fn datetime_plus_duration(date: i64, nanos_of_day: i64, dur: &DurationValue) -> (i64, i64) {
    // Add the signed months (day-clamped) then the whole days, both on the date.
    let days = crate::calendar::add_months_to_days(date, dur.months) + dur.days;
    // Then the sub-day time (seconds + nanos), carrying whole-day overflow into
    // the date. Done in i128 so a large-second duration can't overflow. (#1011)
    let total_nanos =
        i128::from(nanos_of_day) + i128::from(dur.seconds) * 1_000_000_000 + i128::from(dur.nanos);
    let day_ns = i128::from(DAY_NANOS);
    let carry = (total_nanos.div_euclid(day_ns)) as i64;
    let nod = (total_nanos.rem_euclid(day_ns)) as i64;
    (days + carry, nod)
}

/// Parse an ISO-8601 duration into a [`DurationValue`].
fn parse_duration(s: &str) -> Option<DurationValue> {
    let rest = s.strip_prefix('P')?;
    if rest.is_empty() {
        return None; // a bare `P` has no components
    }
    // The alternative `P<date>T<time>` form (`P2012-02-02T14:37:21`) is
    // digits-and-separators only. A designator duration can ALSO contain `-`
    // (a negative component, e.g. `P12Y5M-14DT16H`), so route to the
    // alternative parser only when the date segment has no unit letters
    // (Y/M/W/D/H/S) — a `:` in the time always means the alternative form.
    let date_seg = rest.split('T').next().unwrap_or(rest);
    let alternative = rest.contains(':')
        || (date_seg.contains('-') && !date_seg.bytes().any(|b| b.is_ascii_alphabetic()));
    if alternative {
        return parse_duration_alternative(rest);
    }
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    let mut acc = (0.0_f64, 0.0_f64, 0.0_f64);
    parse_designators(date_part, false, &mut acc)?;
    if let Some(t) = time_part {
        if t.is_empty() {
            return None; // a bare `T` with no time components is malformed
        }
        parse_designators(t, true, &mut acc)?;
    }
    Some(approximate_duration(acc.0, acc.1, acc.2))
}

/// Parse the alternative `P<date>T<time>` duration form, where the "date"
/// components count years/months/days (not a calendar date).
fn parse_duration_alternative(rest: &str) -> Option<DurationValue> {
    let (date_str, time_str) = rest.split_once('T')?;
    let [years, months, days] = date_str.split('-').collect::<Vec<_>>()[..] else {
        return None;
    };
    let [hours, minutes, secs] = time_str.split(':').collect::<Vec<_>>()[..] else {
        return None;
    };
    let years: i64 = years.parse().ok()?;
    let months: i64 = months.parse().ok()?;
    let days: i64 = days.parse().ok()?;
    let hours: f64 = hours.parse().ok()?;
    let minutes: f64 = minutes.parse().ok()?;
    let secs: f64 = secs.parse().ok()?;
    #[allow(
        clippy::cast_possible_truncation,
        reason = "alternative-form times are small, well within i64 nanos"
    )]
    Some(DurationValue::from_total_nanos(
        years * 12 + months,
        days,
        ((hours * 3600.0 + minutes * 60.0 + secs) * 1e9).round() as i64,
    ))
}

/// Scan `{number}{unit}` designator pairs (e.g. `14D`, `0.75M`) into the
/// `(months, days, seconds)` f64 accumulator. `in_time` selects the time-part
/// meaning of `M` (minutes vs months) and `H`/`S`. Fractional larger units are
/// left in their own accumulator and carried down later by
/// [`approximate_duration`], so `P0.75M` yields whole days, not raw seconds.
fn parse_designators(s: &str, in_time: bool, acc: &mut (f64, f64, f64)) -> Option<()> {
    let mut chars = s.chars().peekable();
    while chars.peek().is_some() {
        let mut num = String::new();
        // A component may carry a leading sign — `toString` renders negative
        // components verbatim (`P12Y5M-14DT16H`), and parsing must round-trip
        // them (#920 Temporal6).
        if matches!(chars.peek(), Some('-' | '+')) {
            num.push(chars.next()?);
        }
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                num.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if num.is_empty() || num == "-" || num == "+" {
            return None;
        }
        let unit = chars.next()?;
        let val: f64 = num.parse().ok()?;
        match (in_time, unit) {
            (false, 'Y') => acc.0 += val * 12.0,
            (false, 'M') => acc.0 += val,
            (false, 'W') => acc.1 += val * 7.0,
            (false, 'D') => acc.1 += val,
            (true, 'H') => acc.2 += val * 3600.0,
            (true, 'M') => acc.2 += val * 60.0,
            (true, 'S') => acc.2 += val,
            _ => return None,
        }
    }
    Some(())
}

/// Render a [`Duration`] canonically as `P[nY][nM][nD]T[nH][nM][nS]`. Years are
/// split off the month count; whole days spilling out of the seconds field are
/// folded into the day count; subsecond values trim trailing zeros.
fn format_duration(dur: &DurationValue) -> String {
    use std::fmt::Write as _;
    let years = dur.months / 12;
    let months = dur.months % 12;
    let days = dur.days;
    // The stored form FLOORS `seconds` with a non-negative `nanos`; reconstruct
    // the truncated-toward-zero split for rendering so every H/M/S component
    // shares the sub-day total's sign (`-23h59m59.9s` → `PT-23H-59M-59.9S`, not a
    // mix). Borrow back the floored second when the total is negative; this never
    // overflows (a huge span has `nanos == 0`, so no adjustment).
    let (mut secs, mut sub_ns) = (dur.seconds, dur.nanos);
    if secs < 0 && sub_ns > 0 {
        secs += 1;
        sub_ns -= 1_000_000_000;
    }
    // Split the whole sub-day `seconds` into H/M/S (each `%`/`/` truncates toward
    // zero). The H/M/S group and the day count are INDEPENDENT in openCypher — a
    // 32h sub-day time renders `PT32H`, never folded to `P1DT8H` (#920).
    // `seconds: i64` holds billion-year spans where a single total-nanos field
    // would overflow (#1011).
    let hours = secs / 3600;
    let rem = secs % 3600;
    let minutes = rem / 60;
    let whole_secs = rem % 60;

    let mut out = String::from("P");
    if years != 0 {
        write!(out, "{years}Y").unwrap();
    }
    if months != 0 {
        write!(out, "{months}M").unwrap();
    }
    if days != 0 {
        write!(out, "{days}D").unwrap();
    }
    let mut time = String::new();
    if hours != 0 {
        write!(time, "{hours}H").unwrap();
    }
    if minutes != 0 {
        write!(time, "{minutes}M").unwrap();
    }
    if whole_secs != 0 || sub_ns != 0 {
        write!(time, "{}S", format_seconds_int(whole_secs, sub_ns)).unwrap();
    }
    if !time.is_empty() {
        out.push('T');
        out.push_str(&time);
    }
    if out == "P" {
        out.push_str("T0S");
    }
    out
}

/// Render a duration's seconds component from an integer whole-seconds count and
/// sub-second nanoseconds — both share the duration's sign — trimming trailing
/// zeros: `10`, `49.5`, `-1.999`, `-0.001`. (#920)
fn format_seconds_int(whole_secs: i64, sub_ns: i64) -> String {
    if sub_ns == 0 {
        return whole_secs.to_string();
    }
    let neg = whole_secs < 0 || sub_ns < 0;
    let mut frac = format!("{:09}", sub_ns.unsigned_abs());
    while frac.ends_with('0') {
        frac.pop();
    }
    format!(
        "{}{}.{}",
        if neg { "-" } else { "" },
        whole_secs.unsigned_abs(),
        frac
    )
}

/// Parse a Cypher ISO-8601 date string into a [`NaiveDate`].
///
/// Accepts every form the TCK's `date(<string>)` outline exercises:
///
/// | input         | meaning                | example      |
/// |---------------|------------------------|--------------|
/// | `2015-07-21`  | calendar (extended)    | `2015-07-21` |
/// | `20150721`    | calendar (basic)       | `2015-07-21` |
/// | `2015-07`     | year-month             | `2015-07-01` |
/// | `201507`      | year-month (basic)     | `2015-07-01` |
/// | `2015`        | year only              | `2015-01-01` |
/// | `2015-W30-2`  | ISO week + weekday     | `2015-07-21` |
/// | `2015W302`    | ISO week + weekday     | `2015-07-21` |
/// | `2015-W30`    | ISO week (Monday)      | `2015-07-20` |
/// | `2015W30`     | ISO week (Monday)      | `2015-07-20` |
/// | `2015-202`    | ordinal (day of year)  | `2015-07-21` |
/// | `2015202`     | ordinal (basic)        | `2015-07-21` |
///
/// Returns `None` for any string that is not one of these forms or that names
/// an out-of-range date.
#[must_use]
pub fn parse_date_string(input: &str) -> Option<i64> {
    use crate::calendar;
    let s = input.trim();
    // An optional leading sign for the ISO-8601 expanded-year form
    // (`-999999999-01-01`, `+10000-01-01`); the remainder parses as a positive
    // year and the sign is applied. (The pre-#1011 parser split on the first `-`,
    // so a leading-minus year parsed to an empty head and failed — part of why
    // Temporal10 [9] returned null even before the chrono range limit.)
    let (neg, had_sign, s) = if let Some(r) = s.strip_prefix('-') {
        (true, true, r)
    } else if let Some(r) = s.strip_prefix('+') {
        (false, true, r)
    } else {
        (false, false, s)
    };
    let signed = |y: i64| if neg { -y } else { y };

    // ISO week dates carry a 'W'. Everything before it is the year; everything
    // after (dashes stripped) is `ww` followed by an optional weekday digit.
    if let Some(wpos) = s.find('W') {
        let year: i64 = s[..wpos].trim_end_matches('-').parse().ok()?;
        let rest: String = s[wpos + 1..].chars().filter(|c| *c != '-').collect();
        // `ww` (+ optional weekday digit). Require ASCII digits so the byte
        // slicing below cannot land inside a multi-byte char (would panic).
        if rest.len() < 2 || !rest.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let week: u32 = rest[..2].parse().ok()?;
        let weekday: u32 = if rest.len() > 2 {
            rest[2..].parse().ok()?
        } else {
            1
        };
        return calendar::from_iso_ywd(signed(year), week, weekday);
    }

    if let Some((head, tail)) = s.split_once('-') {
        // Extended forms: `YYYY-MM-DD`, `YYYY-MM`, or `YYYY-DDD` (ordinal).
        let year: i64 = head.parse().ok()?;
        return match tail.split_once('-') {
            Some((month, day)) => {
                calendar::ymd_to_days(signed(year), month.parse().ok()?, day.parse().ok()?)
            }
            None if tail.len() == 3 => calendar::from_ordinal(signed(year), tail.parse().ok()?),
            None => calendar::ymd_to_days(signed(year), tail.parse().ok()?, 1),
        };
    }

    // Basic (dash-free) forms, disambiguated by digit count. These assume a
    // FIXED 4-digit year, so a stripped expanded-year sign can't apply — a signed
    // value must use an extended (dashed) or `W` form. Rejecting here avoids
    // mis-slicing `+10000202` as year `1000` (#1011).
    if !had_sign && !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        let year: i64 = s.get(..4)?.parse().ok()?;
        return match s.len() {
            8 => calendar::ymd_to_days(signed(year), s[4..6].parse().ok()?, s[6..8].parse().ok()?),
            7 => calendar::from_ordinal(signed(year), s[4..].parse().ok()?),
            6 => calendar::ymd_to_days(signed(year), s[4..6].parse().ok()?, 1),
            4 => calendar::ymd_to_days(signed(year), 1, 1),
            _ => None,
        };
    }

    None
}

/// Canonical openCypher rendering of a date (i64 days): `YYYY-MM-DD`, signed for
/// the expanded-year range (#1011). Delegates to [`crate::calendar`].
#[must_use]
pub fn format_date(days: i64) -> String {
    crate::calendar::format_date(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> String {
        format_date(parse_date_string(s).expect("should parse"))
    }

    #[test]
    fn calendar_forms() {
        assert_eq!(d("2015-07-21"), "2015-07-21");
        assert_eq!(d("20150721"), "2015-07-21");
    }

    #[test]
    fn truncate_date_units() {
        // Expected values mirror the openCypher Temporal9 [1] table.
        let t = |s: &str, unit: &str| {
            format_date(
                truncate_date(parse_date_string(s).expect("parse"), unit).expect("truncate"),
            )
        };
        assert_eq!(t("2017-10-11", "millennium"), "2000-01-01");
        assert_eq!(t("1984-10-11", "century"), "1900-01-01");
        assert_eq!(t("1984-10-11", "decade"), "1980-01-01");
        assert_eq!(t("1984-10-11", "year"), "1984-01-01");
        // ISO week-year: 1984-02-01 → Monday of ISO week 1 of 1984.
        assert_eq!(t("1984-02-01", "weekYear"), "1984-01-02");
        assert_eq!(t("1984-11-11", "quarter"), "1984-10-01");
        assert_eq!(t("1984-10-11", "month"), "1984-10-01");
        // 1984-10-11 is a Thursday → Monday of its week is 1984-10-08.
        assert_eq!(t("1984-10-11", "week"), "1984-10-08");
        assert_eq!(t("1984-10-11", "day"), "1984-10-11");
        assert!(truncate_date(parse_date_string("1984-10-11").unwrap(), "bogus").is_none());
    }

    #[test]
    fn extreme_year_date_operations_are_range_complete() {
        // #1011 regression guard: beyond chrono's ±262k-year range, the date
        // read/truncate/projection path must return the TRUE value, not NULL
        // (the value already parses, stores, and renders range-complete).
        let hi = parse_date_string("+999999999-06-15").expect("parse extreme +year");
        assert_eq!(date_component(hi, "year"), Some(999_999_999));
        assert_eq!(date_component(hi, "month"), Some(6));
        assert_eq!(date_component(hi, "day"), Some(15));
        assert_eq!(date_component(hi, "quarter"), Some(2));
        // weekDay/ordinalDay/dayOfQuarter also resolve (no chrono clamp).
        assert!(date_component(hi, "weekDay").is_some());
        assert!(date_component(hi, "ordinalDay").is_some());
        assert!(date_component(hi, "dayOfQuarter").is_some());
        // truncate to year start.
        assert_eq!(
            format_date(truncate_date(hi, "year").unwrap()),
            "+999999999-01-01"
        );
        assert_eq!(
            format_date(truncate_date(hi, "month").unwrap()),
            "+999999999-06-01"
        );
        // projection: replace the day, keep the extreme year/month.
        let projected = project_date(
            hi,
            &DateOverrides {
                day: Some(1),
                ..DateOverrides::default()
            },
        );
        assert_eq!(format_date(projected.unwrap()), "+999999999-06-01");
        // deep-negative year round-trips too.
        let lo = parse_date_string("-999999999-01-01").expect("parse extreme -year");
        assert_eq!(date_component(lo, "year"), Some(-999_999_999));
    }

    #[test]
    fn duration_subsecond_accessors_dont_overflow_on_huge_spans() {
        // #1011 regression guard: a >~292-year `seconds` makes `seconds * 1e9`
        // overflow i64 — the cumulative sub-second accessors must return NULL
        // (unrepresentable) rather than panic (debug) or silently wrap (release).
        let huge = DurationValue::from_total_nanos(0, 0, 0);
        let huge = DurationValue {
            seconds: 31_556_951_999_913_600, // ~1e9-year inSeconds span
            ..huge
        };
        assert_eq!(duration_component(&huge, "nanoseconds"), None);
        assert_eq!(duration_component(&huge, "milliseconds"), None);
        assert_eq!(duration_component(&huge, "microseconds"), None);
        // The non-cumulative accessors still work.
        assert_eq!(
            duration_component(&huge, "seconds"),
            Some(31_556_951_999_913_600)
        );
        // A small duration's cumulative accessors are unaffected.
        let small = DurationValue::from_total_nanos(0, 0, 1_500_000_000);
        assert_eq!(duration_component(&small, "milliseconds"), Some(1_500));
    }

    #[test]
    fn signed_basic_dateless_form_is_rejected_not_misparsed() {
        // #1011 regression guard: a stripped +/- sign must NOT fall through to the
        // fixed-4-digit-year basic branch (which mis-sliced `+10000202` → year
        // 1000). Signed years are only valid in the extended (dashed) / `W` forms.
        assert_eq!(parse_date_string("+10000202"), None);
        assert_eq!(parse_date_string("-10000202"), None);
        // The equivalent extended ordinal form still parses correctly.
        assert_eq!(
            format_date(parse_date_string("+10000-202").expect("extended ordinal")),
            "+10000-07-20"
        );
    }

    #[test]
    fn truncate_time_units() {
        // 12:31:14.645876123 = 45_074_645_876_123 ns since midnight.
        let n = 45_074_645_876_123_i64;
        let r =
            |unit: &str| render_localtime_nanos(truncate_time_nanos(n, unit).expect("truncate"));
        assert_eq!(r("hour"), "12:00");
        assert_eq!(r("minute"), "12:31");
        assert_eq!(r("second"), "12:31:14");
        assert_eq!(r("millisecond"), "12:31:14.645");
        assert_eq!(r("microsecond"), "12:31:14.645876");
        // A unit coarser than a time-of-day truncates to midnight.
        assert_eq!(r("day"), "00:00");
        assert_eq!(r("year"), "00:00");
        assert!(truncate_time_nanos(n, "bogus").is_none());
    }

    #[test]
    fn project_localtime_keeps_unspecified_subsecond_groups() {
        // #920: a sub-second override replaces only the named ms/µs/ns group and
        // keeps the rest of the base. `truncate('millisecond')` floors to .645,
        // then `{nanosecond: 2}` must yield .645000002, not .000000002.
        let base = 45_074_645_000_000_i64; // 12:31:14.645
        let only_ns = LocalTimeOverrides {
            hour: None,
            minute: None,
            second: None,
            millisecond: None,
            microsecond: None,
            nanosecond: Some(2),
        };
        assert_eq!(
            render_localtime_nanos(project_localtime(base, &only_ns).unwrap()),
            "12:31:14.645000002"
        );
        // Overriding the millisecond group replaces only it.
        let ms = LocalTimeOverrides {
            millisecond: Some(123),
            ..only_ns
        };
        assert_eq!(
            render_localtime_nanos(project_localtime(base, &ms).unwrap()),
            "12:31:14.123000002"
        );
    }

    #[test]
    fn duration_str_parses_negative_components_round_trip() {
        // #920 Temporal6: `toString` renders negative components verbatim, and
        // `duration(<that string>)` must parse them back to the same value.
        let secs = 16 * 3600; // 16h
        let v = DurationValue::from_total_nanos(149, -14, i64::from(secs) * 1_000_000_000);
        assert_eq!(duration_value_from_str("P12Y5M-14DT16H"), Some(v));
        // Round-trip: render then parse yields the original components.
        assert_eq!(duration_value_from_str(&render_duration_value(&v)), Some(v));
    }

    #[test]
    fn duration_construction_carries_rounded_up_subsecond() {
        // #1011 regression: a fractional second that rounds to 1e9 nanos must
        // carry into `seconds`, not leave nanos == 1_000_000_000 (which rendered
        // as a bogus "PT0.1S"). `duration({seconds: 0.9999999996})` → PT1S.
        let fields: Fields =
            [("seconds".to_string(), TemporalField::Float(0.999_999_999_6))].into();
        let dv = build_duration_map(&fields).expect("duration");
        assert!(dv.nanos >= 0 && dv.nanos < 1_000_000_000, "nanos canonical");
        assert_eq!((dv.seconds, dv.nanos), (1, 0));
        assert_eq!(render_duration_value(&dv), "PT1S");
    }

    #[test]
    fn duration_render_preserves_subsecond_for_large_spans() {
        // #920 Temporal10: a huge sub-day span must keep exact sub-second digits
        // (an f64 seconds count would corrupt `.142` to `.14199996`).
        let nanos = (278_565_i64 * 3600 + 45 * 60 + 22) * 1_000_000_000 + 142_000_000;
        let dv = |n: i64| DurationValue::from_total_nanos(0, 0, n);
        assert_eq!(render_duration_value(&dv(nanos)), "PT278565H45M22.142S");
        // Sign-consistent sub-second rendering.
        assert_eq!(render_duration_value(&dv(-1_999_000_000)), "PT-1.999S");
        assert_eq!(render_duration_value(&dv(-1_000_000)), "PT-0.001S");
    }

    #[test]
    fn duration_scale_matches_opencypher() {
        // Base P12Y5M14DT16H13M10.000000001S (Temporal8 [7]).
        let base = DurationValue::from_total_nanos(149, 14, 58_390_000_000_001);
        // * 2: pure component doubling; the sub-day time stays as 32H (no day-fold).
        assert_eq!(
            scale_duration(&base, 2.0, false),
            DurationValue::from_total_nanos(298, 28, 116_780_000_000_002),
            "P24Y10M28DT32H26M20.000000002S"
        );
        // / 2: a fractional month (0.5) overflows to 15.2184375 days, the
        // fractional day to seconds — 74mo, 22d, 48068s = 13H21M8S.
        assert_eq!(
            scale_duration(&base, 2.0, true),
            DurationValue::from_total_nanos(74, 22, 48_068_000_000_000),
            "P6Y2M22DT13H21M8S"
        );
        // * 0.5 equals / 2.
        assert_eq!(
            scale_duration(&base, 0.5, false),
            DurationValue::from_total_nanos(74, 22, 48_068_000_000_000)
        );
    }

    #[test]
    fn duration_arithmetic() {
        let d = |s: &str| parse_date_string(s).unwrap();
        // Temporal8 [1]: date + duration{12y5mo14d16h12m70s2ns} → '1997-03-25'
        // (months=149, days=14, sub-day time < 24h so no extra day).
        let nanos = (16 * 3600 + 12 * 60 + 70) * 1_000_000_000 + 2;
        let dur = |m, dd, n| DurationValue::from_total_nanos(m, dd, n);
        assert_eq!(
            date_plus_duration(d("1984-10-11"), &dur(149, 14, nanos)),
            d("1997-03-25")
        );
        // Subtraction (negated components) → '1972-04-27'.
        assert_eq!(
            date_plus_duration(d("1984-10-11"), &dur(-149, -14, -nanos)),
            d("1972-04-27")
        );
        // Temporal8 [1] row 3: the duration's sub-day time exceeds 24h
        // (122293.5s ≈ 1d10h), so a WHOLE day carries into the date even though
        // the date drops the sub-day remainder: 155mo + 29d + 1d = 1997-10-11.
        let big = 122_293_500_000_000_i64;
        assert_eq!(
            date_plus_duration(d("1984-10-11"), &dur(155, 29, big)),
            d("1997-10-11")
        );
        assert_eq!(
            date_plus_duration(d("1984-10-11"), &dur(-155, -29, -big)),
            d("1971-10-12")
        );
        // localtime wraps mod 24h.
        let day = 86_400_000_000_000_i64;
        assert_eq!(
            localtime_plus_duration(23 * 3_600_000_000_000, 2 * 3_600_000_000_000),
            3_600_000_000_000
        );
        assert_eq!(localtime_plus_duration(0, -1), day - 1);
        // localdatetime: sub-day time carries into the date.
        let (date, nod) = datetime_plus_duration(
            d("1984-10-11"),
            12 * 3_600_000_000_000,
            &dur(0, 0, 13 * 3_600_000_000_000),
        );
        assert_eq!((date, nod), (d("1984-10-12"), 3_600_000_000_000)); // 12:00 + 13h → next day 01:00
    }

    #[test]
    fn time_value_from_str_defaults_offset_to_utc() {
        // #920: a bare `time('14:30')` (no offset) defaults to UTC (offset 0).
        assert_eq!(
            time_value_from_str("14:30"),
            Some((14 * 3_600_000_000_000 + 30 * 60_000_000_000, 0))
        );
        // An explicit offset is honoured.
        assert_eq!(
            time_value_from_str("14:30+01:00"),
            Some((14 * 3_600_000_000_000 + 30 * 60_000_000_000, 3600))
        );
    }

    #[test]
    fn duration_between_units() {
        let date = |s: &str| (Some(parse_date_string(s).unwrap()), 0_i64, None, None);
        let lt = |h: i64, m: i64| (None, (h * 3600 + m * 60) * 1_000_000_000, None, None);
        let dv = |m, dd, n| DurationValue::from_total_nanos(m, dd, n);
        // date → date: calendar split (Temporal10 [2]).
        assert_eq!(
            duration_between(
                &date("1984-10-11"),
                &date("2015-06-24"),
                BetweenMode::Between
            ),
            Some(dv(368, 13, 0)) // 30Y8M13D
        );
        // inMonths drops the days; inDays gives the whole-day total.
        assert_eq!(
            duration_between(
                &date("1984-10-11"),
                &date("2015-06-24"),
                BetweenMode::Months
            ),
            Some(dv(368, 0, 0))
        );
        assert_eq!(
            duration_between(&date("1984-10-11"), &date("2015-06-24"), BetweenMode::Days),
            Some(dv(0, 11213, 0))
        );
        // time-only → just the time-of-day diff, no month/day span.
        assert_eq!(
            duration_between(&lt(14, 30), &lt(16, 30), BetweenMode::Between),
            Some(dv(0, 0, 2 * 3_600_000_000_000))
        );
        // Negative direction: months/days/seconds/nanos share the sign.
        let r = duration_between(
            &date("2015-06-24"),
            &date("1984-10-11"),
            BetweenMode::Between,
        )
        .unwrap();
        assert!(r.months <= 0 && r.days <= 0 && r.seconds <= 0 && r.nanos <= 0);

        // Sub-month backward span crossing an earlier day-of-month: months must
        // be 0, not -1 (#920 — the whole_months span-direction fix). 22h back.
        let ldt = |s: &str| {
            let (d, t) = s.split_once('T').unwrap();
            (
                Some(parse_date_string(d).unwrap()),
                nanos_of_day(&parse_time_of_day(t).unwrap()),
                None,
                None,
            )
        };
        assert_eq!(
            duration_between(
                &ldt("2018-01-02T10:00:00"),
                &ldt("2018-01-01T12:00:00"),
                BetweenMode::Between,
            ),
            Some(dv(0, 0, -22 * 3_600_000_000_000)),
            "a 22h backward span is PT-22H, not P-1M30DT2H"
        );
    }

    #[test]
    fn duration_inseconds_dst_named_zone() {
        // #1007 Temporal10 [8]: the Stockholm fall-back day (2017-10-29) has 25
        // wall-clock hours, so an unzoned operand resolved in that named zone
        // yields the real elapsed span — not the naive wall-clock difference.
        let d = |y, m, day| crate::calendar::ymd_to_days(y, m, day).unwrap();
        let h = |n: i64| n * 3_600_000_000_000;
        // datetime(2017-10-29T00:00[Europe/Stockholm], +02:00) vs localdatetime 04:00.
        let zoned = (
            Some(d(2017, 10, 29)),
            0_i64,
            Some(7200_i32),
            Some("Europe/Stockholm".to_string()),
        );
        let unzoned_0429_04 = (Some(d(2017, 10, 29)), h(4), None, None);
        assert_eq!(
            duration_between(&zoned, &unzoned_0429_04, BetweenMode::Seconds),
            Some(DurationValue::from_total_nanos(0, 0, h(5))),
            "00:00 (+02) → 04:00 across the fall-back is 5 real hours"
        );
        // datetime(...00:00 Stockholm) vs date(2017-10-30) → a full 25-hour day.
        let next_date = (Some(d(2017, 10, 30)), 0_i64, None, None);
        assert_eq!(
            duration_between(&zoned, &next_date, BetweenMode::Seconds),
            Some(DurationValue::from_total_nanos(0, 0, h(25)))
        );
    }

    #[test]
    fn localtime_str_round_trip() {
        // Mirrors Temporal2 [2]: pure-value rendering must reproduce these.
        let r = |s: &str| render_localtime_nanos(localtime_nanos_from_str(s).expect("parse"));
        assert_eq!(r("21:40:32.142"), "21:40:32.142");
        assert_eq!(r("214032.142"), "21:40:32.142");
        assert_eq!(r("21:40:32"), "21:40:32");
        assert_eq!(r("21:40"), "21:40");
        assert_eq!(r("21"), "21:00");
        // A localtime carries no offset.
        assert!(localtime_nanos_from_str("21:40:32+01:00").is_none());
    }

    #[test]
    fn localtime_of_day_from_any_temporal_string() {
        let r = |s: &str| render_localtime_nanos(time_of_day_nanos_any(s).expect("parse"));
        assert_eq!(r("12:31:14.645876123"), "12:31:14.645876123"); // localtime
        assert_eq!(r("12:31:14.645876+01:00"), "12:31:14.645876"); // time → drop offset
        assert_eq!(r("1984-10-11T12:31:14.645"), "12:31:14.645"); // localdatetime
        assert_eq!(r("1984-10-11T12:00+01:00[Europe/Stockholm]"), "12:00"); // datetime named-zone
    }

    #[test]
    fn localtime_projection_overrides() {
        let base = localtime_nanos_from_str("12:31:14.645876123").unwrap();
        let proj =
            |o: &LocalTimeOverrides| render_localtime_nanos(project_localtime(base, o).unwrap());
        assert_eq!(proj(&LocalTimeOverrides::default()), "12:31:14.645876123");
        assert_eq!(
            proj(&LocalTimeOverrides {
                second: Some(42),
                ..Default::default()
            }),
            "12:31:42.645876123"
        );
    }

    #[test]
    fn localdatetime_str_round_trip() {
        // Mirrors Temporal2 [4]: pure-value rendering must reproduce these.
        let r = |s: &str| {
            let (d, n) = localdatetime_parts_from_str(s).expect("parse");
            render_localdatetime(d, n)
        };
        assert_eq!(r("2015-07-21T21:40:32.142"), "2015-07-21T21:40:32.142");
        assert_eq!(r("2015-W30-2T214032.142"), "2015-07-21T21:40:32.142");
        assert_eq!(r("2015-202T21:40:32"), "2015-07-21T21:40:32");
        assert_eq!(r("2015T214032"), "2015-01-01T21:40:32");
        assert_eq!(r("20150721T21:40"), "2015-07-21T21:40");
        assert_eq!(r("2015202T21"), "2015-07-21T21:00");
        // A localdatetime carries no zone.
        assert!(localdatetime_parts_from_str("2015-07-21T21:40:32+01:00").is_none());
    }

    #[test]
    fn localdatetime_full_year_range() {
        // The two-field representation spans years an i64-nanosecond timestamp
        // cannot (it overflows ~year 2262) — regression guard for WithOrderBy1.
        let r = |s: &str| {
            let (d, n) = localdatetime_parts_from_str(s).expect("parse");
            render_localdatetime(d, n)
        };
        assert_eq!(
            r("0001-01-01T01:01:01.000000001"),
            "0001-01-01T01:01:01.000000001"
        );
        assert_eq!(
            r("9999-09-09T09:59:59.999999999"),
            "9999-09-09T09:59:59.999999999"
        );
    }

    #[test]
    fn time_str_round_trip() {
        // Mirrors Temporal2 [3]: pure-value rendering must reproduce these.
        let r = |s: &str| {
            let (n, o) = time_value_from_str(s).expect("parse");
            render_time_value(n, o)
        };
        assert_eq!(r("21:40:32.142+0100"), "21:40:32.142+01:00");
        assert_eq!(r("214032.142Z"), "21:40:32.142Z");
        assert_eq!(r("214032-0100"), "21:40:32-01:00");
        assert_eq!(r("21:40-01:30"), "21:40-01:30");
        assert_eq!(r("2140-00:00"), "21:40Z"); // -00:00 → UTC
        assert_eq!(r("2140-02"), "21:40-02:00");
        assert_eq!(r("22+18:00"), "22:00+18:00");
        // A bare time (no offset) defaults to UTC (Z) per openCypher (#920).
        assert_eq!(r("21:40:32"), "21:40:32Z");
    }

    #[test]
    fn time_projection_zone_shift_vs_attach() {
        let base = |s: &str| time_of_day_with_offset(s).expect("parse");
        let ren = |(n, o): (i64, i32)| render_time_value(n, o);
        let plus5 = Some(5 * 3600);
        // localtime base (no offset) + new zone → ATTACH (no shift).
        let (n, off) = base("12:31:14.645876123");
        assert_eq!(ren(project_time(n, off, plus5)), "12:31:14.645876123+05:00");
        // time base (+01:00) + new zone +05:00 → SHIFT (preserve instant).
        let (n, off) = base("12:31:14.645876+01:00");
        assert_eq!(ren(project_time(n, off, plus5)), "16:31:14.645876+05:00");
        // No new zone: keep the base offset, or UTC for an offset-less base.
        let (n, off) = base("12:31:14.645876+01:00");
        assert_eq!(ren(project_time(n, off, None)), "12:31:14.645876+01:00");
        let (n, off) = base("12:31:14.645876123");
        assert_eq!(ren(project_time(n, off, None)), "12:31:14.645876123Z");
    }

    #[test]
    fn datetime_str_round_trip() {
        let r = |s: &str| {
            let (d, n, o, z) = datetime_value_from_str(s).expect("parse");
            render_datetime_value(d, n, o, z.as_deref())
        };
        assert_eq!(
            r("2015-07-21T21:40:32.142+01:00"),
            "2015-07-21T21:40:32.142+01:00"
        );
        assert_eq!(r("2015-07-21T21:40Z"), "2015-07-21T21:40Z");
        // Named zone: offset resolved at that instant (summer +02:00).
        assert_eq!(
            r("2017-08-08T12:31:14.645876123[Europe/Stockholm]"),
            "2017-08-08T12:31:14.645876123+02:00[Europe/Stockholm]"
        );
    }

    #[test]
    fn datetime_projection_shift_and_attach() {
        let render = |p: Option<DateTimeParts>| {
            let (d, n, o, z) = p.unwrap();
            render_datetime_value(d, n, o, z.as_deref())
        };
        let date = parse_date_string("1984-10-11").unwrap();
        let nanos = nanos_of_day(&parse_time_of_day("12:31:14.645876").unwrap());
        // time base (+01:00) shifted to +05:00 → wall time advances 4h.
        assert_eq!(
            render(project_datetime(
                date,
                nanos,
                Some(3600),
                None,
                Some("+05:00")
            )),
            "1984-10-11T16:31:14.645876+05:00"
        );
        // time base (+01:00) shifted to a NAMED zone (Honolulu -10:00) → 01:31.
        assert_eq!(
            render(project_datetime(
                date,
                nanos,
                Some(3600),
                None,
                Some("Pacific/Honolulu")
            )),
            "1984-10-11T01:31:14.645876-10:00[Pacific/Honolulu]"
        );
        // localtime base (no offset) + named zone → ATTACH (no shift).
        assert_eq!(
            render(project_datetime(
                date,
                nanos,
                None,
                None,
                Some("Pacific/Honolulu")
            )),
            "1984-10-11T12:31:14.645876-10:00[Pacific/Honolulu]"
        );
        // datetime base, no new zone → keep the source offset + zone label.
        assert_eq!(
            render(project_datetime(
                date,
                nanos,
                Some(3600),
                Some("Europe/Stockholm"),
                None
            )),
            "1984-10-11T12:31:14.645876+01:00[Europe/Stockholm]"
        );
    }

    #[test]
    fn partial_forms() {
        assert_eq!(d("2015-07"), "2015-07-01");
        assert_eq!(d("201507"), "2015-07-01");
        assert_eq!(d("2015"), "2015-01-01");
    }

    #[test]
    fn week_forms() {
        assert_eq!(d("2015-W30-2"), "2015-07-21");
        assert_eq!(d("2015W302"), "2015-07-21");
        assert_eq!(d("2015-W30"), "2015-07-20");
        assert_eq!(d("2015W30"), "2015-07-20");
    }

    #[test]
    fn ordinal_forms() {
        assert_eq!(d("2015-202"), "2015-07-21");
        assert_eq!(d("2015202"), "2015-07-21");
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_date_string("not-a-date").is_none());
        assert!(parse_date_string("2015-13-01").is_none());
        assert!(parse_date_string("").is_none());
    }

    #[test]
    fn non_ascii_week_does_not_panic() {
        // Multi-byte chars after 'W' must not panic on byte slicing.
        assert!(parse_date_string("2015W€0").is_none());
        assert!(parse_date_string("2015W3é").is_none());
    }

    #[test]
    fn local_time_forms() {
        let r = |s| render_local_time(s).unwrap();
        assert_eq!(r("21:40:32.142"), "21:40:32.142");
        assert_eq!(r("214032.142"), "21:40:32.142");
        assert_eq!(r("21:40:32"), "21:40:32");
        assert_eq!(r("214032"), "21:40:32");
        assert_eq!(r("21:40"), "21:40");
        assert_eq!(r("2140"), "21:40");
        assert_eq!(r("21"), "21:00");
        // A local time may not carry an offset.
        assert!(render_local_time("21:40Z").is_none());
    }

    #[test]
    fn time_forms() {
        let r = |s| render_time(s).unwrap();
        assert_eq!(r("21:40:32.142+0100"), "21:40:32.142+01:00");
        assert_eq!(r("214032.142Z"), "21:40:32.142Z");
        assert_eq!(r("21:40:32+01:00"), "21:40:32+01:00");
        assert_eq!(r("214032-0100"), "21:40:32-01:00");
        assert_eq!(r("21:40-01:30"), "21:40-01:30");
        assert_eq!(r("2140-00:00"), "21:40Z"); // zero offset renders as Z
        assert_eq!(r("2140-02"), "21:40-02:00");
        assert_eq!(r("22+18:00"), "22:00+18:00");
        // An offset is required.
        assert!(render_time("21:40").is_none());
    }

    #[test]
    fn local_date_time_forms() {
        let r = |s| render_local_date_time(s).unwrap();
        assert_eq!(r("2015-07-21T21:40:32.142"), "2015-07-21T21:40:32.142");
        assert_eq!(r("2015-W30-2T214032.142"), "2015-07-21T21:40:32.142");
        assert_eq!(r("2015-202T21:40:32"), "2015-07-21T21:40:32");
        assert_eq!(r("2015T214032"), "2015-01-01T21:40:32");
        assert_eq!(r("20150721T21:40"), "2015-07-21T21:40");
        assert_eq!(r("2015-W30T2140"), "2015-07-20T21:40");
        assert_eq!(r("2015202T21"), "2015-07-21T21:00");
    }

    #[test]
    fn date_time_forms() {
        let r = |s| render_date_time(s).unwrap();
        assert_eq!(
            r("2015-07-21T21:40:32.142+0100"),
            "2015-07-21T21:40:32.142+01:00"
        );
        assert_eq!(r("2015-W30-2T214032.142Z"), "2015-07-21T21:40:32.142Z");
        assert_eq!(r("2015-202T21:40:32+01:00"), "2015-07-21T21:40:32+01:00");
        assert_eq!(r("2015T214032-0100"), "2015-01-01T21:40:32-01:00");
        assert_eq!(r("20150721T21:40-01:30"), "2015-07-21T21:40-01:30");
        assert_eq!(r("2015-W30T2140-00:00"), "2015-07-20T21:40Z");
        assert_eq!(r("2015-W30T2140-02"), "2015-07-20T21:40-02:00");
        assert_eq!(r("2015202T21+18:00"), "2015-07-21T21:00+18:00");
    }

    #[test]
    fn date_time_named_zone_forms() {
        let r = |s| render_date_time(s).unwrap();
        // Explicit offset is echoed (reformatted); zone preserved.
        assert_eq!(
            r("2015-07-21T21:40:32.142+02:00[Europe/Stockholm]"),
            "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]"
        );
        assert_eq!(
            r("2015-07-21T21:40:32.142+0845[Australia/Eucla]"),
            "2015-07-21T21:40:32.142+08:45[Australia/Eucla]"
        );
        assert_eq!(
            r("2015-07-21T21:40:32.142-04[America/New_York]"),
            "2015-07-21T21:40:32.142-04:00[America/New_York]"
        );
        // No offset → resolved from the zone (London in July → BST +01:00).
        assert_eq!(
            r("2015-07-21T21:40:32.142[Europe/London]"),
            "2015-07-21T21:40:32.142+01:00[Europe/London]"
        );
        // Historical LMT before standard time (Stockholm 1818 → +00:53:28).
        assert_eq!(
            r("1818-07-21T21:40:32.142[Europe/Stockholm]"),
            "1818-07-21T21:40:32.142+00:53:28[Europe/Stockholm]"
        );
    }

    #[test]
    fn duration_forms() {
        let r = |s| render_duration(s).unwrap();
        assert_eq!(r("P14DT16H12M"), "P14DT16H12M");
        assert_eq!(r("P5M1.5D"), "P5M1DT12H");
        assert_eq!(r("P0.75M"), "P22DT19H51M49.5S");
        assert_eq!(r("PT0.75M"), "PT45S");
        assert_eq!(r("P2.5W"), "P17DT12H");
        assert_eq!(r("P12Y5M14DT16H12M70S"), "P12Y5M14DT16H13M10S");
        assert_eq!(r("P2012-02-02T14:37:21.545"), "P2012Y2M2DT14H37M21.545S");
    }

    fn fields(pairs: &[(&str, TemporalField)]) -> Fields {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    (*k).to_string(),
                    match v {
                        TemporalField::Int(n) => TemporalField::Int(*n),
                        TemporalField::Float(x) => TemporalField::Float(*x),
                        TemporalField::Str(s) => TemporalField::Str(s.clone()),
                        TemporalField::Date(d) => TemporalField::Date(*d),
                    },
                )
            })
            .collect()
    }

    fn int(n: i64) -> TemporalField {
        TemporalField::Int(n)
    }

    #[test]
    fn date_map_forms() {
        let r = |p: &[(&str, TemporalField)]| render_temporal_map("date", &fields(p)).unwrap();
        assert_eq!(
            r(&[("year", int(1984)), ("month", int(10)), ("day", int(11))]),
            "1984-10-11"
        );
        assert_eq!(r(&[("year", int(1984)), ("month", int(10))]), "1984-10-01");
        assert_eq!(
            r(&[
                ("year", int(1984)),
                ("week", int(10)),
                ("dayOfWeek", int(3))
            ]),
            "1984-03-07"
        );
        assert_eq!(r(&[("year", int(1984)), ("week", int(10))]), "1984-03-05");
        assert_eq!(r(&[("year", int(1984))]), "1984-01-01");
        assert_eq!(
            r(&[("year", int(1984)), ("ordinalDay", int(202))]),
            "1984-07-20"
        );
        assert_eq!(
            r(&[
                ("year", int(1984)),
                ("quarter", int(3)),
                ("dayOfQuarter", int(45))
            ]),
            "1984-08-14"
        );
        // ISO-week year vs calendar year
        assert_eq!(r(&[("year", int(1817)), ("week", int(1))]), "1816-12-30");
    }

    #[test]
    fn date_map_anchored_week() {
        let anchor = TemporalField::Date(parse_date_string("1816-12-31").unwrap());
        let out =
            render_temporal_map("date", &fields(&[("date", anchor), ("week", int(2))])).unwrap();
        assert_eq!(out, "1817-01-07");
    }

    #[test]
    fn time_and_subsecond_maps() {
        let lt =
            |p: &[(&str, TemporalField)]| render_temporal_map("localtime", &fields(p)).unwrap();
        assert_eq!(
            lt(&[
                ("hour", int(12)),
                ("minute", int(31)),
                ("second", int(14)),
                ("nanosecond", int(789)),
                ("millisecond", int(123)),
                ("microsecond", int(456)),
            ]),
            "12:31:14.123456789"
        );
        assert_eq!(lt(&[("hour", int(12))]), "12:00");
        // time() defaults to Z, honours an offset
        let t = |p: &[(&str, TemporalField)]| render_temporal_map("time", &fields(p)).unwrap();
        assert_eq!(t(&[("hour", int(12)), ("minute", int(31))]), "12:31Z");
        assert_eq!(
            t(&[
                ("hour", int(12)),
                ("minute", int(34)),
                ("second", int(56)),
                ("timezone", TemporalField::Str("+02:05:59".into())),
            ]),
            "12:34:56+02:05:59"
        );
    }

    #[test]
    fn datetime_maps_with_zones() {
        let dt = |p: &[(&str, TemporalField)]| render_temporal_map("datetime", &fields(p)).unwrap();
        // default zone Z
        assert_eq!(
            dt(&[("year", int(1984)), ("month", int(10)), ("day", int(11))]),
            "1984-10-11T00:00Z"
        );
        // offset zone
        assert_eq!(
            dt(&[
                ("year", int(1984)),
                ("ordinalDay", int(202)),
                ("hour", int(12)),
                ("timezone", TemporalField::Str("+01:00".into())),
            ]),
            "1984-07-20T12:00+01:00"
        );
        // named zone — summer (CEST) vs winter (CET)
        assert_eq!(
            dt(&[
                ("year", int(1984)),
                ("ordinalDay", int(202)),
                ("hour", int(12)),
                ("timezone", TemporalField::Str("Europe/Stockholm".into())),
            ]),
            "1984-07-20T12:00+02:00[Europe/Stockholm]"
        );
        assert_eq!(
            dt(&[
                ("year", int(1984)),
                ("month", int(10)),
                ("day", int(11)),
                ("hour", int(12)),
                ("timezone", TemporalField::Str("Europe/Stockholm".into())),
            ]),
            "1984-10-11T12:00+01:00[Europe/Stockholm]"
        );
    }

    #[test]
    fn duration_maps() {
        let d = |p: &[(&str, TemporalField)]| render_temporal_map("duration", &fields(p)).unwrap();
        assert_eq!(
            d(&[("days", int(14)), ("hours", int(16)), ("minutes", int(12))]),
            "P14DT16H12M"
        );
        assert_eq!(
            d(&[("months", int(5)), ("days", TemporalField::Float(1.5))]),
            "P5M1DT12H"
        );
        assert_eq!(
            d(&[("months", TemporalField::Float(0.75))]),
            "P22DT19H51M49.5S"
        );
        assert_eq!(d(&[("weeks", TemporalField::Float(2.5))]), "P17DT12H");
        assert_eq!(
            d(&[
                ("days", int(14)),
                ("seconds", int(70)),
                ("nanoseconds", int(1))
            ]),
            "P14DT1M10.000000001S"
        );
        assert_eq!(
            d(&[("minutes", TemporalField::Float(1.5)), ("seconds", int(1))]),
            "PT1M31S"
        );
    }

    #[test]
    fn epoch_constructors() {
        assert_eq!(
            render_from_epoch(416_779, 999_999_999).unwrap(),
            "1970-01-05T19:46:19.999999999Z"
        );
        assert_eq!(
            render_from_epoch_millis(237_821_673_987).unwrap(),
            "1977-07-15T13:34:33.987Z"
        );
    }

    #[test]
    fn rejects_malformed_temporals() {
        // Sub-nanosecond fraction (>9 digits) is rejected, not truncated.
        assert!(render_local_time("21:40:32.1234567890").is_none());
        // Signed/garbage offset components are rejected.
        assert!(render_time("21:40+01:-30").is_none());
        assert!(render_time("21:40+24:00").is_none());
        // A bare `P` duration has no components.
        assert!(render_duration("P").is_none());
    }

    #[test]
    fn date_components() {
        // Temporal5 [1]: 1984-10-11.
        let d = parse_date_string("1984-10-11").unwrap();
        let c = |n| date_component(d, n).unwrap();
        assert_eq!(c("year"), 1984);
        assert_eq!(c("quarter"), 4);
        assert_eq!(c("month"), 10);
        assert_eq!(c("week"), 41);
        assert_eq!(c("weekYear"), 1984);
        assert_eq!(c("day"), 11);
        assert_eq!(c("ordinalDay"), 285);
        assert_eq!(c("weekDay"), 4);
        assert_eq!(c("dayOfQuarter"), 11);
        // Temporal5 [2]: 1984-01-01 falls in the last ISO week of 1983.
        let d2 = parse_date_string("1984-01-01").unwrap();
        assert_eq!(date_component(d2, "weekYear").unwrap(), 1983);
        assert_eq!(date_component(d2, "week").unwrap(), 52);
        assert_eq!(date_component(d2, "weekDay").unwrap(), 7);
        assert!(date_component(d, "bogus").is_none());
    }
}
