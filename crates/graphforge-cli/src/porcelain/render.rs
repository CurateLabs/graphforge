//! Log entries and their one-line rendering.
use serde::Serialize;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

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

/// The Versions of a log in `git log --topo-order` order: every child before
/// any of its parents, newest first among those ready, then in discovery
/// order. `parents` is `None` for a Version the history only names (a boundary
/// anchor): it ends that line without an entry. Without `all` only the first
/// parent is followed.
pub(super) fn history_order<K: Ord + Clone>(
    start: K,
    all: bool,
    parents: impl Fn(&K) -> Option<Vec<K>>,
    stamp: impl Fn(&K) -> Option<i64>,
) -> Vec<K> {
    let mut discovered = vec![start.clone()];
    let mut index = BTreeMap::from([(start, 0_usize)]);
    let mut edges: Vec<Vec<usize>> = Vec::new();
    let mut known = Vec::new();
    let mut at = 0;
    while at < discovered.len() {
        let version = discovered[at].clone();
        let next = parents(&version);
        known.push(next.is_some());
        let mut to = Vec::new();
        for parent in next
            .unwrap_or_default()
            .into_iter()
            .take(if all { usize::MAX } else { 1 })
        {
            let slot = *index.entry(parent.clone()).or_insert_with(|| {
                discovered.push(parent);
                discovered.len() - 1
            });
            to.push(slot);
        }
        edges.push(to);
        at += 1;
    }
    let mut waiting = vec![0_usize; discovered.len()];
    for (from, to) in edges.iter().enumerate() {
        if known[from] {
            for &parent in to {
                waiting[parent] += 1;
            }
        }
    }
    let mut ready: BinaryHeap<(Option<i64>, Reverse<usize>)> = BinaryHeap::new();
    ready.push((stamp(&discovered[0]), Reverse(0)));
    let mut order = Vec::new();
    while let Some((_, Reverse(slot))) = ready.pop() {
        if known[slot] {
            order.push(discovered[slot].clone());
        }
        for &parent in &edges[slot] {
            waiting[parent] -= 1;
            if waiting[parent] == 0 {
                ready.push((stamp(&discovered[parent]), Reverse(parent)));
            }
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::{date, history_order};
    use std::collections::BTreeMap;

    #[test]
    fn history_orders_children_before_shared_parents_with_newest_ready_first() {
        // The base has the largest timestamp (clock skew); topology still wins.
        let parents = BTreeMap::from([
            ("merge", vec!["left", "right"]),
            ("left", vec!["base"]),
            ("right", vec!["middle"]),
            ("middle", vec!["base"]),
            ("base", vec![]),
        ]);
        let stamps = BTreeMap::from([
            ("merge", 4),
            ("left", 1),
            ("right", 3),
            ("middle", 2),
            ("base", 9),
        ]);
        let walk = |all| {
            history_order(
                "merge",
                all,
                |v| parents.get(v).cloned(),
                |v| stamps.get(v).copied(),
            )
        };
        assert_eq!(walk(true), ["merge", "right", "middle", "left", "base"]);
        assert_eq!(walk(false), ["merge", "left", "base"]);
    }

    #[test]
    fn a_boundary_ends_only_its_line_and_shared_versions_appear_once() {
        let parents = BTreeMap::from([
            ("merge", vec!["left", "right"]),
            ("left", vec!["boundary"]),
            ("right", vec!["other", "boundary"]),
            ("other", vec!["boundary"]),
        ]);
        assert_eq!(
            history_order("merge", true, |v| parents.get(v).cloned(), |_| None),
            ["merge", "left", "right", "other"]
        );
        assert_eq!(
            history_order("merge", false, |v| parents.get(v).cloned(), |_| None),
            ["merge", "left"]
        );
        assert!(history_order("boundary", true, |v| parents.get(v).cloned(), |_| None).is_empty());
    }

    #[test]
    fn dates_are_utc_calendar_days() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(951_782_400_000_000), "2000-02-29");
        assert_eq!(date(1_760_000_000_000_000), "2025-10-09");
        assert_eq!(date(-1), "1969-12-31");
    }
}
