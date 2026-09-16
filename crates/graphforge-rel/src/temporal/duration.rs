//! Duration construction, normalization, arithmetic, and calendar-aware differences.

use super::{Fields, f_num, resolve_zone_offset, time_parts_from_nanos};

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
pub(super) fn build_duration_map(f: &Fields) -> Option<DurationValue> {
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
pub(super) fn format_duration(dur: &DurationValue) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::test_support::{fields, int};
    use crate::temporal::{
        TemporalField, nanos_of_day, parse_date_string, parse_time_of_day, render_temporal_map,
    };

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
}
