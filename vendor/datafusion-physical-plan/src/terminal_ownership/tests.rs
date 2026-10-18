// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg(feature = "test_utils")]

use super::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{MutexGuard, PoisonError};

// The production recorder is intentionally process-global. Every test in this
// module holds this guard for the entire Session lifetime, including cleanup.
static TEST_SESSION: Mutex<()> = Mutex::new(());
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn session_guard() -> MutexGuard<'static, ()> {
    TEST_SESSION.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "graphforge-ownership-tests-{}-{ordinal}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn lines(&self, name: &str) -> Vec<String> {
        std::fs::read_to_string(self.path(name))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn envelope() -> Envelope {
    Envelope {
        calls: 2,
        slots: 1,
        descriptors: 8,
        edges: 1,
        columns: 1,
        nodes: 2,
        depth: 2,
        buffers: 4,
    }
}

fn kind(line: &str, expected: &str) -> bool {
    line.contains(&format!("\"kind\":\"{expected}\""))
}

fn number(line: &str, field: &str) -> u64 {
    let prefix = format!("\"{field}\":");
    let value = line.split_once(&prefix).unwrap().1;
    let digits = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    value[..digits].parse().unwrap()
}

fn complete_empty_call(session: &Session) {
    session.begin_call().unwrap();
    session.snapshot(0);
    session.snapshot(0);
    session.end_call(0);
}

#[cfg_attr(test, test)]
fn sequential_calls_preserve_generation_and_counters_without_reset() {
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = envelope();
    let bounds = limits.bounds().unwrap();
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    let mut ids = Vec::new();
    for call in 1..=2 {
        assert_eq!(session.begin_call().unwrap(), call);
        let owner = Life::new(Kind::Batch, 0, 0);
        assert_ne!(owner.id(), 0);
        ids.push(owner.id());
        session.snapshot(0);
        drop(owner);
        session.snapshot(0);
        session.end_call(0);
    }
    assert!(ids[1] > ids[0]);
    assert_eq!(with(|r| (r.constructed, r.retired)), Some((2, 2)));
    session.finish().unwrap();
    let lines = directory.lines("capture");
    assert_eq!(lines.iter().filter(|l| kind(l, "session")).count(), 1);
    assert_eq!(lines.iter().filter(|l| kind(l, "run_end")).count(), 2);
    for call in 1..=2 {
        let cuts: Vec<_> = lines
            .iter()
            .filter(|l| kind(l, "snapshot_begin") && number(l, "call") == call)
            .collect();
        assert_eq!(cuts.len(), 2);
        assert_eq!(
            (number(cuts[0], "snapshot"), number(cuts[1], "snapshot")),
            (1, 2)
        );
        assert!(cuts.iter().all(|l| number(l, "constructed") == call));
        assert_eq!(number(cuts[0], "retired"), call - 1);
        assert_eq!(number(cuts[1], "retired"), call);
    }
    assert!(lines.last().unwrap().contains("\"complete\":true"));
    assert!(lines.len() <= bounds.records);
    assert!(
        std::fs::metadata(directory.path("capture")).unwrap().len()
            <= bounds.bytes as u64
    );
}

#[cfg_attr(test, test)]
fn retirement_reuses_one_live_slot_across_many_generations() {
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = envelope();
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    let mut previous = 0;
    for _ in 0..2 {
        session.begin_call().unwrap();
        for _ in 0..512 {
            let owner = Life::new(Kind::Source, 0, 0);
            assert!(owner.id() > previous);
            previous = owner.id();
            assert_eq!(with(|r| r.owners.iter().flatten().count()), Some(1));
            drop(owner);
            assert_eq!(with(|r| r.owners.iter().flatten().count()), Some(0));
        }
        session.snapshot(0);
        session.snapshot(0);
        session.end_call(0);
    }
    assert_eq!(
        with(|r| (r.constructed, r.retired, r.poison)),
        Some((1024, 1024, 0))
    );
    session.finish().unwrap();
    let lines = directory.lines("capture");
    // Retired generations do not accumulate event records or retained owners.
    assert!(!lines.iter().any(|l| kind(l, "owner")));
    assert_eq!(lines.iter().filter(|l| kind(l, "snapshot_end")).count(), 4);
    assert!(lines.len() <= limits.bounds().unwrap().records);
}

#[cfg_attr(test, test)]
fn capture_creation_is_exclusive_for_active_session_and_existing_file() {
    let _guard = session_guard();
    let directory = Directory::new();
    let first = directory.path("first");
    let second = directory.path("second");
    let session = Session::create(&first, envelope()).unwrap();
    let error = Session::create(&second, envelope()).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert!(!second.exists());
    drop(session);
    assert_eq!(
        Session::create(&first, envelope()).err().unwrap().kind(),
        io::ErrorKind::AlreadyExists
    );
    let session = Session::create(&second, envelope()).unwrap();
    complete_empty_call(&session);
    complete_empty_call(&session);
    session.finish().unwrap();
}

#[cfg_attr(test, test)]
fn dropping_session_leaves_no_commit_footer_and_releases_global_capture() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("abandoned"), envelope()).unwrap();
    session.begin_call().unwrap();
    session.snapshot(0);
    drop(session);
    let lines = directory.lines("abandoned");
    assert!(lines.iter().any(|l| kind(l, "session")));
    assert!(!lines.iter().any(|l| kind(l, "session_end")));
    let next = Session::create(&directory.path("next"), envelope()).unwrap();
    complete_empty_call(&next);
    complete_empty_call(&next);
    next.finish().unwrap();
}

#[cfg_attr(test, test)]
fn invalid_zero_and_overflow_envelopes_are_rejected_before_file_creation() {
    let _guard = session_guard();
    let directory = Directory::new();
    let invalid = [
        Envelope {
            calls: 0,
            ..envelope()
        },
        Envelope {
            slots: 0,
            ..envelope()
        },
        Envelope {
            descriptors: 0,
            ..envelope()
        },
        Envelope {
            columns: 0,
            ..envelope()
        },
        Envelope {
            nodes: 0,
            ..envelope()
        },
        Envelope {
            depth: 0,
            ..envelope()
        },
        Envelope {
            buffers: 0,
            ..envelope()
        },
        Envelope {
            calls: usize::MAX,
            ..envelope()
        },
        Envelope {
            slots: usize::MAX,
            ..envelope()
        },
        Envelope {
            descriptors: usize::MAX,
            ..envelope()
        },
        Envelope {
            edges: usize::MAX,
            ..envelope()
        },
    ];
    for (index, limits) in invalid.into_iter().enumerate() {
        assert_eq!(
            limits.bounds().err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        let path = directory.path(&format!("invalid-{index}"));
        assert_eq!(
            Session::create(&path, limits).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!path.exists());
    }
}

#[cfg_attr(test, test)]
fn live_owner_overflow_poison_persists_after_owners_retire() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("capture"), envelope()).unwrap();
    session.begin_call().unwrap();
    let first = Life::new(Kind::Batch, 0, 0);
    let excess = Life::new(Kind::Batch, 0, 0);
    assert_ne!(first.id(), 0);
    assert_eq!(excess.id(), 0);
    assert_eq!(with(|r| r.poison & POISON_CAPACITY), Some(POISON_CAPACITY));
    drop(excess);
    drop(first);
    session.snapshot(0);
    session.snapshot(0);
    session.end_call(0);
    complete_empty_call(&session);
    assert!(session.finish().is_err());
    let lines = directory.lines("capture");
    assert!(
        lines
            .iter()
            .filter(|l| kind(l, "snapshot_end"))
            .all(|l| l.contains("\"complete\":false"))
    );
    assert!(lines.last().unwrap().contains("\"complete\":false"));
}

#[cfg_attr(test, test)]
fn snapshot_during_open_transition_is_explicitly_incomplete() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("capture"), envelope()).unwrap();
    session.begin_call().unwrap();
    let transition = Transition::begin();
    session.snapshot(0);
    drop(transition);
    session.snapshot(0);
    session.end_call(0);
    drop(session);
    let lines = directory.lines("capture");
    let begins: Vec<_> = lines.iter().filter(|l| kind(l, "snapshot_begin")).collect();
    assert_eq!(number(begins[0], "in_flight"), 1);
    assert_eq!(number(begins[1], "in_flight"), 0);
    assert!(number(begins[1], "epoch") > number(begins[0], "epoch"));
    let ends: Vec<_> = lines.iter().filter(|l| kind(l, "snapshot_end")).collect();
    assert!(ends[0].contains("\"complete\":false"));
    assert!(ends[1].contains("\"complete\":true"));
}

#[cfg_attr(test, test)]
fn calls_without_exactly_two_snapshots_cannot_commit() {
    let _guard = session_guard();
    let directory = Directory::new();
    for count in [0, 1, 3] {
        let name = format!("snapshots-{count}");
        let session = Session::create(
            &directory.path(&name),
            Envelope {
                calls: 1,
                ..envelope()
            },
        )
        .unwrap();
        session.begin_call().unwrap();
        for _ in 0..count {
            session.snapshot(0);
        }
        session.end_call(0);
        assert_eq!(with(|r| r.poison & POISON_PROTOCOL), Some(POISON_PROTOCOL));
        assert!(session.finish().is_err());
        assert!(
            directory
                .lines(&name)
                .last()
                .unwrap()
                .contains("\"complete\":false")
        );
    }
}

#[cfg_attr(test, test)]
fn finishing_with_missing_or_open_call_cannot_commit() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("missing"), envelope()).unwrap();
    complete_empty_call(&session);
    assert!(session.finish().is_err());
    let session = Session::create(&directory.path("open"), envelope()).unwrap();
    session.begin_call().unwrap();
    session.snapshot(0);
    session.snapshot(0);
    assert!(session.finish().is_err());
    for name in ["missing", "open"] {
        assert!(
            directory
                .lines(name)
                .last()
                .unwrap()
                .contains("\"complete\":false")
        );
    }
}

#[cfg_attr(test, test)]
fn arithmetic_counter_overflow_poison_does_not_wrap_generation() {
    let _guard = session_guard();
    let mut value = u64::MAX;
    let mut poison = 0;
    Recorder::count(&mut value, &mut poison);
    assert_eq!(value, u64::MAX);
    assert_eq!(poison, POISON_ARITHMETIC);
}

#[cfg(target_os = "linux")]
#[cfg_attr(test, test)]
fn file_write_failure_poison_prevents_commit() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("capture"), envelope()).unwrap();
    // create_new correctly rejects /dev/full itself. Replace only the private
    // recorder sink after a real exclusive session has written its manifest.
    let full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    with(|r| r.file = full).unwrap();
    complete_empty_call(&session);
    complete_empty_call(&session);
    assert_eq!(with(|r| r.poison & POISON_IO), Some(POISON_IO));
    assert_eq!(with(|r| r.records), Some(1));
    assert!(session.finish().is_err());
    let lines = directory.lines("capture");
    assert_eq!(lines.len(), 1);
    assert!(kind(&lines[0], "session"));
}

/// Execute these controls from the existing workspace product test lane.
pub(super) fn run_controls() {
    sequential_calls_preserve_generation_and_counters_without_reset();
    retirement_reuses_one_live_slot_across_many_generations();
    capture_creation_is_exclusive_for_active_session_and_existing_file();
    dropping_session_leaves_no_commit_footer_and_releases_global_capture();
    invalid_zero_and_overflow_envelopes_are_rejected_before_file_creation();
    live_owner_overflow_poison_persists_after_owners_retire();
    snapshot_during_open_transition_is_explicitly_incomplete();
    calls_without_exactly_two_snapshots_cannot_commit();
    finishing_with_missing_or_open_call_cannot_commit();
    arithmetic_counter_overflow_poison_does_not_wrap_generation();
    #[cfg(target_os = "linux")]
    file_write_failure_poison_prevents_commit();
}
