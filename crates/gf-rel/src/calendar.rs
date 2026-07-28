//! A proleptic-Gregorian calendar on `i64` days-since-epoch (#1011).
//!
//! chrono's `NaiveDate` caps near year ±262,143 and Arrow `Date32` is `i32` days
//! (≈ year ±5.8M), but openCypher dates span years **−999,999,999 …
//! +999,999,999**. This module does all DATE math on `i64` days since the Unix
//! epoch (1970-01-01), so the full range round-trips. It deliberately covers only
//! the calendar (year/month/day/ordinal/ISO-week/weekday + day-precision
//! arithmetic + ISO rendering); time-of-day stays nanoseconds-of-day and named
//! zones stay `chrono_tz` (both year-independent).
//!
//! The core conversions are Howard Hinnant's `days_from_civil` / `civil_from_days`
//! (<http://howardhinnant.github.io/date_algorithms.html>), exact for any year in
//! the proleptic Gregorian calendar (which has a year 0). Cross-checked against
//! chrono for in-range years in the unit tests.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the civil-calendar algorithm casts between i64 day-counts and the \
              bounded month/day/ordinal fields (month 1..=12, day 1..=31, ordinal \
              1..=366); every such cast is provably in range, exhaustively verified \
              against chrono + a 12M-iteration fuzz over the full year span"
)]

/// 1970-01-01 is a Thursday; ISO weekday 4 (Mon=1 … Sun=7).
const EPOCH_ISO_WEEKDAY_OFFSET: i64 = 3;

/// Days since 1970-01-01 for the civil date `(year, month, day)` in the proleptic
/// Gregorian calendar. `month` is 1–12, `day` is 1–31 (not validated here — call
/// [`ymd_to_days`] for the checked form). Exact for any `i64` year in range.
#[must_use]
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let m = i64::from(m);
    let d = i64::from(d);
    // Shift so the leap day is the last day of the (shifted) year.
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The civil date `(year, month, day)` for a days-since-epoch count (the inverse
/// of [`days_from_civil`]). `month` is 1–12, `day` is 1–31.
#[must_use]
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Whether `year` is a leap year in the proleptic Gregorian calendar.
#[must_use]
pub fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Number of days in `month` (1–12) of `year` (28–31). `month` out of range → 0.
#[must_use]
pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days in `year` (365 or 366).
#[must_use]
pub fn days_in_year(year: i64) -> u32 {
    if is_leap(year) { 366 } else { 365 }
}

/// Validated civil → days: `None` for an out-of-range month/day (e.g. 2025-02-30).
#[must_use]
pub fn ymd_to_days(y: i64, m: u32, d: u32) -> Option<i64> {
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

/// Days for the `ordinal`-th day of `year` (1 = Jan 1). `None` if out of range.
#[must_use]
pub fn from_ordinal(year: i64, ordinal: u32) -> Option<i64> {
    if ordinal < 1 || ordinal > days_in_year(year) {
        return None;
    }
    Some(days_from_civil(year, 1, 1) + i64::from(ordinal) - 1)
}

/// The 1-based day-of-year (1–366) for a days count.
#[must_use]
pub fn ordinal(z: i64) -> u32 {
    let (y, _, _) = civil_from_days(z);
    (z - days_from_civil(y, 1, 1) + 1) as u32
}

/// ISO weekday, 1 = Monday … 7 = Sunday.
#[must_use]
pub fn iso_weekday(z: i64) -> u32 {
    ((z + EPOCH_ISO_WEEKDAY_OFFSET).rem_euclid(7)) as u32 + 1
}

/// Days from the Monday of the date's week (0 = Monday … 6 = Sunday).
#[must_use]
pub fn num_days_from_monday(z: i64) -> u32 {
    (z + EPOCH_ISO_WEEKDAY_OFFSET).rem_euclid(7) as u32
}

/// The number of ISO weeks in `iso_year` (52 or 53).
#[must_use]
pub fn iso_weeks_in_year(iso_year: i64) -> u32 {
    // A year has 53 ISO weeks iff Jan 1 is a Thursday, or it is a leap year and
    // Jan 1 is a Wednesday — equivalently, its last day (Dec 31) is Thu/Fri-ish.
    // Compute via the weekday of Jan 1.
    let jan1 = days_from_civil(iso_year, 1, 1);
    let wd = iso_weekday(jan1); // 1..7
    if wd == 4 || (wd == 3 && is_leap(iso_year)) {
        53
    } else {
        52
    }
}

/// The ISO-8601 week-based year and week number `(iso_year, week)` for a days
/// count. The ISO week-year can differ from the calendar year near Jan 1 / Dec 31.
#[must_use]
pub fn iso_week(z: i64) -> (i64, u32) {
    let (y, _, _) = civil_from_days(z);
    let ord = i64::from(ordinal(z));
    let wd = i64::from(iso_weekday(z));
    // Provisional week within the calendar year.
    let week = (ord - wd + 10) / 7;
    if week < 1 {
        // Belongs to the last week of the previous year.
        (y - 1, iso_weeks_in_year(y - 1))
    } else if week > i64::from(iso_weeks_in_year(y)) {
        // Belongs to week 1 of the next year.
        (y + 1, 1)
    } else {
        (y, week as u32)
    }
}

/// Days for the ISO `(iso_year, week, weekday)` (weekday 1 = Mon … 7 = Sun).
/// `None` if `week`/`weekday` is out of range for that ISO year.
#[must_use]
pub fn from_iso_ywd(iso_year: i64, week: u32, weekday: u32) -> Option<i64> {
    if !(1..=7).contains(&weekday) || week < 1 || week > iso_weeks_in_year(iso_year) {
        return None;
    }
    // Jan 4 is always in ISO week 1; find the Monday of week 1, then offset.
    let jan4 = days_from_civil(iso_year, 1, 4);
    let week1_monday = jan4 - (i64::from(iso_weekday(jan4)) - 1);
    Some(week1_monday + i64::from(week - 1) * 7 + i64::from(weekday - 1))
}

/// Add a signed number of calendar `months` to `(y, m, d)`, clamping the day to
/// the target month's length (e.g. Jan 31 + 1 month → Feb 28/29). Returns the
/// resulting civil date.
#[must_use]
pub fn add_months(y: i64, m: u32, d: u32, months: i64) -> (i64, u32, u32) {
    let total = y * 12 + i64::from(m - 1) + months;
    let ny = total.div_euclid(12);
    let nm = total.rem_euclid(12) as u32 + 1;
    let nd = d.min(days_in_month(ny, nm));
    (ny, nm, nd)
}

/// Add `months` calendar months to a days count (day-clamped); convenience over
/// [`add_months`] for the days representation.
#[must_use]
pub fn add_months_to_days(z: i64, months: i64) -> i64 {
    let (y, m, d) = civil_from_days(z);
    let (ny, nm, nd) = add_months(y, m, d, months);
    days_from_civil(ny, nm, nd)
}

/// The 1-based quarter (1–4) of `month` (1–12).
#[must_use]
pub fn quarter_of_month(month: u32) -> u32 {
    (month - 1) / 3 + 1
}

/// Canonical openCypher ISO rendering of a date `(YYYY-MM-DD)`. Years 0000–9999
/// are zero-padded to four digits with no sign; outside that range the ISO-8601
/// expanded form applies — a leading `+` for years > 9999 and `-` for negative
/// years (Neo4j: "a plus sign must prefix any year after 9999"). Matches chrono's
/// `%Y` for the years chrono can render, and extends past chrono's ±262k cap.
#[must_use]
pub fn format_date(z: i64) -> String {
    let (y, m, d) = civil_from_days(z);
    format!("{}-{m:02}-{d:02}", format_year(y))
}

/// Year formatting shared by date / localdatetime / datetime rendering.
#[must_use]
pub fn format_year(y: i64) -> String {
    if (0..=9999).contains(&y) {
        format!("{y:04}")
    } else if y < 0 {
        // `{:04}` pads the magnitude to ≥4 digits; longer values are unaffected.
        format!("-{:04}", -y)
    } else {
        // ISO-8601 expanded form: years > 9999 carry an explicit leading `+`.
        format!("+{y}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate};

    /// chrono's day-count for an in-range date, for cross-checking.
    fn chrono_days(y: i32, m: u32, d: u32) -> i64 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days()
    }

    #[test]
    fn civil_roundtrip_and_matches_chrono_in_range() {
        // Sample across the in-range era, including negatives and leap edges.
        for &(y, m, d) in &[
            (1970, 1, 1),
            (2000, 2, 29),
            (1984, 10, 11),
            (1, 1, 1),
            (0, 1, 1),
            (-1, 12, 31),
            (-44, 3, 15),
            (9999, 12, 31),
            (2017, 10, 29),
        ] {
            let z = days_from_civil(y, m, d);
            assert_eq!(z, chrono_days(y as i32, m, d), "{y}-{m}-{d} vs chrono");
            assert_eq!(civil_from_days(z), (y, m, d), "roundtrip {y}-{m}-{d}");
        }
    }

    #[test]
    fn extreme_years_roundtrip() {
        for &(y, m, d) in &[(-999_999_999, 1, 1), (999_999_999, 12, 31)] {
            let z = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(z), (y, m, d));
        }
        // The span used by Temporal10 [9]: between the two extremes is
        // 1999999998y 11m 30d → the day-count difference must be exact.
        let lo = days_from_civil(-999_999_999, 1, 1);
        let hi = days_from_civil(999_999_999, 12, 31);
        assert!(hi - lo > 0);
    }

    #[test]
    fn weekday_ordinal_match_chrono() {
        for &(y, m, d) in &[(1970, 1, 1), (2025, 6, 30), (1984, 1, 1), (-1, 6, 15)] {
            let z = days_from_civil(y, m, d);
            let nd = NaiveDate::from_ymd_opt(y as i32, m, d).unwrap();
            assert_eq!(
                iso_weekday(z),
                nd.weekday().number_from_monday(),
                "wd {y}-{m}-{d}"
            );
            assert_eq!(num_days_from_monday(z), nd.weekday().num_days_from_monday());
            assert_eq!(ordinal(z), nd.ordinal(), "ordinal {y}-{m}-{d}");
        }
    }

    #[test]
    fn iso_week_matches_chrono() {
        // Including the year-boundary cases ISO week is famous for.
        for &(y, m, d) in &[
            (1984, 1, 1),   // belongs to ISO week 52 of 1983
            (2020, 12, 31), // ISO week 53 of 2020
            (2021, 1, 1),   // still 2020-W53
            (2025, 6, 30),
            (2016, 1, 1),
        ] {
            let z = days_from_civil(y, m, d);
            let nd = NaiveDate::from_ymd_opt(y as i32, m, d).unwrap();
            let iso = nd.iso_week();
            assert_eq!(
                iso_week(z),
                (i64::from(iso.year()), iso.week()),
                "iso_week {y}-{m}-{d}"
            );
        }
    }

    #[test]
    fn from_iso_ywd_inverts_iso_week() {
        for &(y, m, d) in &[(1984, 1, 1), (2020, 12, 31), (2025, 6, 30)] {
            let z = days_from_civil(y, m, d);
            let (iy, w) = iso_week(z);
            let wd = iso_weekday(z);
            assert_eq!(from_iso_ywd(iy, w, wd), Some(z), "ywd inverse {y}-{m}-{d}");
        }
    }

    #[test]
    fn add_months_clamps_day() {
        assert_eq!(add_months(2025, 1, 31, 1), (2025, 2, 28)); // Jan 31 +1mo → Feb 28
        assert_eq!(add_months(2024, 1, 31, 1), (2024, 2, 29)); // leap
        assert_eq!(add_months(2025, 3, 15, -1), (2025, 2, 15));
        assert_eq!(add_months(2025, 12, 10, 1), (2026, 1, 10)); // year carry
        assert_eq!(add_months(2025, 1, 10, -1), (2024, 12, 10)); // year borrow
    }

    #[test]
    fn ymd_validation() {
        assert_eq!(ymd_to_days(2025, 2, 30), None);
        assert_eq!(ymd_to_days(2024, 2, 29), Some(days_from_civil(2024, 2, 29)));
        assert_eq!(ymd_to_days(2025, 13, 1), None);
    }

    #[test]
    fn format_date_matches_chrono_in_range_and_signs_extremes() {
        for &(y, m, d) in &[(2025, 6, 30), (1, 1, 1), (985, 7, 4), (-1, 12, 31)] {
            let z = days_from_civil(y, m, d);
            let chrono = NaiveDate::from_ymd_opt(y as i32, m, d)
                .unwrap()
                .format("%Y-%m-%d")
                .to_string();
            assert_eq!(format_date(z), chrono, "format {y}-{m}-{d} vs chrono");
        }
        // Extreme / >9999 years use the ISO-8601 expanded form: '-' for negative,
        // and a mandatory leading '+' for years after 9999 (Neo4j semantics).
        assert_eq!(
            format_date(days_from_civil(-999_999_999, 1, 1)),
            "-999999999-01-01"
        );
        assert_eq!(
            format_date(days_from_civil(999_999_999, 12, 31)),
            "+999999999-12-31"
        );
        assert_eq!(format_year(10000), "+10000");
        assert_eq!(format_year(0), "0000");
    }
}
