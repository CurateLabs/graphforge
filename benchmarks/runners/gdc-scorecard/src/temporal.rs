//! Strict parsing of LDBC date and datetime text into GraphForge's persisted
//! forms: a date is days since the Unix epoch, a datetime is a UTC instant in
//! microseconds. Every rejected value says why; nothing is rounded or guessed.

use crate::mapping::TemporalFormat;

const MILLIS_PER_DAY: i64 = 86_400_000;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// Days since 1970-01-01 of a proleptic Gregorian civil date (Howard
/// Hinnant's `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        _ => 28,
    }
}

fn digits(text: &str, what: &str) -> Result<i64, String> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{what} {text:?} is not a number"));
    }
    text.parse::<i64>()
        .map_err(|_| format!("{what} {text:?} is out of range"))
}

/// `YYYY-MM-DD` as days since the epoch.
fn civil_date(text: &str) -> Result<i64, String> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(format!("date {text:?} is not YYYY-MM-DD"));
    }
    let year = digits(&text[0..4], "year")?;
    let month = digits(&text[5..7], "month")?;
    let day = digits(&text[8..10], "day")?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return Err(format!("date {text:?} does not exist"));
    }
    Ok(days_from_civil(year, month, day))
}

/// `HH:MM:SS[.fraction]` as microseconds of the day. Digits beyond the
/// microsecond must be zero, so no precision is silently dropped.
fn time_of_day(text: &str) -> Result<i64, String> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let bytes = clock.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return Err(format!("time {text:?} is not HH:MM:SS[.fraction]"));
    }
    let hour = digits(&clock[0..2], "hour")?;
    let minute = digits(&clock[3..5], "minute")?;
    let second = digits(&clock[6..8], "second")?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("time {text:?} does not exist"));
    }
    let mut micros = 0;
    if let Some(fraction) = fraction {
        if fraction.is_empty() || fraction.len() > 9 {
            return Err(format!("time {text:?} has a 1-9 digit fraction"));
        }
        digits(fraction, "fraction")?;
        let (kept, dropped) = fraction.split_at(fraction.len().min(6));
        if dropped.bytes().any(|byte| byte != b'0') {
            return Err(format!("time {text:?} is finer than a microsecond"));
        }
        micros = digits(kept, "fraction")? * 10_i64.pow(6 - u32::try_from(kept.len()).unwrap_or(6));
    }
    Ok(((hour * 60 + minute) * 60 + second) * MICROS_PER_SECOND + micros)
}

/// `Z`, `+HH:MM`, `-HH:MM`, `+HHMM` or `-HHMM` as seconds east of UTC.
fn offset_seconds(text: &str) -> Result<i64, String> {
    if text == "Z" {
        return Ok(0);
    }
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'+') => (1, &text[1..]),
        Some(b'-') => (-1, &text[1..]),
        _ => return Err(format!("offset {text:?} is not Z or +HH:MM")),
    };
    let (hours, minutes) = match rest.len() {
        5 if rest.as_bytes()[2] == b':' => (&rest[0..2], &rest[3..5]),
        4 => (&rest[0..2], &rest[2..4]),
        _ => return Err(format!("offset {text:?} is not Z or +HH:MM")),
    };
    let (hours, minutes) = (digits(hours, "offset")?, digits(minutes, "offset")?);
    if hours > 18 || minutes > 59 {
        return Err(format!("offset {text:?} is out of range"));
    }
    Ok(sign * (hours * 3600 + minutes * 60))
}

fn epoch_millis(text: &str) -> Result<i64, String> {
    let digits_only = text.strip_prefix('-').unwrap_or(text);
    if digits_only.is_empty() || !digits_only.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{text:?} is not epoch milliseconds"));
    }
    text.parse::<i64>()
        .map_err(|_| format!("{text:?} is out of range"))
}

fn micros(days: i64, time_micros: i64, offset: i64) -> Result<i64, String> {
    days.checked_mul(86_400 * MICROS_PER_SECOND)
        .and_then(|value| value.checked_add(time_micros))
        .and_then(|value| value.checked_sub(offset * MICROS_PER_SECOND))
        .ok_or_else(|| "datetime is out of range".to_owned())
}

/// Parses a date into days since the Unix epoch.
///
/// # Errors
/// A message naming why `text` is not a date in `format`.
pub fn parse_date(text: &str, format: TemporalFormat) -> Result<i64, String> {
    match format {
        TemporalFormat::Iso8601 => civil_date(text),
        TemporalFormat::NaiveUtc => {
            let (date, time) = text
                .split_once(' ')
                .ok_or_else(|| format!("date {text:?} is not YYYY-MM-DD HH:MM:SS"))?;
            if time_of_day(time)? != 0 {
                return Err(format!("date {text:?} is not at midnight"));
            }
            civil_date(date)
        }
        TemporalFormat::EpochMillis => {
            let millis = epoch_millis(text)?;
            if millis.rem_euclid(MILLIS_PER_DAY) != 0 {
                return Err(format!("date {text:?} is not a whole UTC day"));
            }
            Ok(millis.div_euclid(MILLIS_PER_DAY))
        }
    }
}

/// Parses a datetime into microseconds since the Unix epoch, UTC.
///
/// # Errors
/// A message naming why `text` is not a datetime in `format`.
pub fn parse_datetime(text: &str, format: TemporalFormat) -> Result<i64, String> {
    match format {
        TemporalFormat::Iso8601 => {
            let (date, rest) = text
                .split_once('T')
                .ok_or_else(|| format!("datetime {text:?} has no T separator"))?;
            let split = rest
                .find(['Z', '+', '-'])
                .ok_or_else(|| format!("datetime {text:?} has no UTC offset"))?;
            let (time, offset) = rest.split_at(split);
            micros(civil_date(date)?, time_of_day(time)?, offset_seconds(offset)?)
        }
        TemporalFormat::NaiveUtc => {
            let (date, time) = text
                .split_once(' ')
                .ok_or_else(|| format!("datetime {text:?} is not YYYY-MM-DD HH:MM:SS"))?;
            micros(civil_date(date)?, time_of_day(time)?, 0)
        }
        TemporalFormat::EpochMillis => epoch_millis(text)?
            .checked_mul(1000)
            .ok_or_else(|| format!("{text:?} is out of range")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TemporalFormat::{EpochMillis, Iso8601, NaiveUtc};

    #[test]
    fn civil_days_match_known_dates() {
        assert_eq!(civil_date("1970-01-01"), Ok(0));
        assert_eq!(civil_date("1969-12-31"), Ok(-1));
        assert_eq!(civil_date("2000-03-01"), Ok(11_017));
        assert_eq!(civil_date("1984-03-11"), Ok(5_183));
        assert_eq!(civil_date("2024-02-29"), Ok(19_782));
        for bad in [
            "2023-02-29",
            "1900-02-29",
            "2024-13-01",
            "2024-00-10",
            "2024-04-31",
            "24-01-01",
            "2024/01/01",
            "2024-1-01",
        ] {
            assert!(civil_date(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn dates_parse_in_every_format_and_refuse_partial_days() {
        assert_eq!(parse_date("1984-03-11", Iso8601), Ok(5_183));
        assert_eq!(parse_date("1988-10-17 00:00:00", NaiveUtc), Ok(6_864));
        assert_eq!(parse_date("628646400000", EpochMillis), Ok(7_276));
        assert_eq!(parse_date("-86400000", EpochMillis), Ok(-1));
        assert!(parse_date("1988-10-17 00:00:01", NaiveUtc).is_err());
        assert!(parse_date("628646400001", EpochMillis).is_err());
        assert!(parse_date("1984-03-11T00:00:00Z", Iso8601).is_err());
        assert!(parse_date("12ab", EpochMillis).is_err());
    }

    #[test]
    fn datetimes_are_utc_instants_in_microseconds() {
        let ldbc_bi = parse_datetime("2010-01-03T15:10:41.499+00:00", Iso8601).unwrap();
        assert_eq!(ldbc_bi, 1_262_531_441_499_000);
        assert_eq!(
            parse_datetime("2010-01-03T15:10:41.499Z", Iso8601),
            Ok(ldbc_bi)
        );
        assert_eq!(
            parse_datetime("2010-01-03T17:10:41.499+0200", Iso8601),
            Ok(ldbc_bi)
        );
        assert_eq!(
            parse_datetime("2010-01-03T14:10:41.499-01:00", Iso8601),
            Ok(ldbc_bi)
        );
        assert_eq!(
            parse_datetime("2010-01-03 15:10:41.499", NaiveUtc),
            Ok(ldbc_bi)
        );
        assert_eq!(parse_datetime("1262531441499", EpochMillis), Ok(ldbc_bi));
        assert_eq!(
            parse_datetime("2020-05-05 21:16:49.46", NaiveUtc),
            parse_datetime("2020-05-05 21:16:49.460000000", NaiveUtc)
        );
        assert_eq!(parse_datetime("1970-01-01 00:00:00", NaiveUtc), Ok(0));
        assert_eq!(
            parse_datetime("1970-01-01T00:00:00.000001Z", Iso8601),
            Ok(1)
        );
    }

    #[test]
    fn datetimes_refuse_ambiguity_and_lost_precision() {
        for (text, format) in [
            ("2010-01-03T15:10:41.499", Iso8601),
            ("2010-01-03 15:10:41.499+00:00", Iso8601),
            ("2010-01-03T15:10:41.499+00:00", NaiveUtc),
            ("2010-01-03T15:10:41.4990001Z", Iso8601),
            ("2010-01-03T15:10:41.0000001Z", Iso8601),
            ("2010-01-03T24:00:00Z", Iso8601),
            ("2010-01-03T15:60:00Z", Iso8601),
            ("2010-01-03T15:10:60Z", Iso8601),
            ("2010-01-03T15:10:41.Z", Iso8601),
            ("2010-01-03T15:10:41+19:00", Iso8601),
            ("2010-01-03T15:10:41+1:00", Iso8601),
            ("2010-02-30T15:10:41Z", Iso8601),
            ("1.5", EpochMillis),
            ("99999999999999999", EpochMillis),
        ] {
            assert!(parse_datetime(text, format).is_err(), "{text} {format:?}");
        }
        assert_eq!(
            parse_datetime("2010-01-03T15:10:41.4990Z", Iso8601),
            parse_datetime("2010-01-03T15:10:41.499Z", Iso8601)
        );
    }
}
