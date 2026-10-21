//! Log entries and their one-line rendering.
use serde::Serialize;
use serde_json::Value;

/// One Version in a log, local or published.
#[derive(Debug, Serialize)]
pub(crate) struct LogEntry {
    pub version_uuid: String,
    pub parents: Vec<String>,
    pub author: Option<Value>,
    pub committer: Option<Value>,
    pub created_at: Option<i64>,
    pub label: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub kind: String,
}

fn person(value: Option<&Value>) -> &str {
    value
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("-")
}

/// `<short uuid> (<tags>) <author> <date> <subject>`
pub(crate) fn log_line(entry: &LogEntry) -> String {
    let mut parts: Vec<String> = vec![entry.version_uuid.chars().take(8).collect()];
    if !entry.tags.is_empty() {
        parts.push(format!("({})", entry.tags.join(", ")));
    }
    parts.push(person(entry.author.as_ref()).to_owned());
    parts.extend(entry.created_at.map(date));
    let subject = entry
        .description
        .as_deref()
        .and_then(|text| text.lines().next())
        .or(entry.label.as_deref())
        .unwrap_or("");
    if !subject.is_empty() {
        parts.push(subject.to_owned());
    }
    parts.join(" ")
}

/// UTC `YYYY-MM-DD` of a microsecond timestamp.
fn date(micros: i64) -> String {
    let days = micros.div_euclid(86_400_000_000);
    // Civil-from-days (Howard Hinnant), proleptic Gregorian.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::date;

    #[test]
    fn dates_are_utc_calendar_days() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(951_782_400_000_000), "2000-02-29");
        assert_eq!(date(1_760_000_000_000_000), "2025-10-09");
        assert_eq!(date(-1), "1969-12-31");
    }
}
