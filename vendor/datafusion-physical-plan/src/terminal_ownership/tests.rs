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
fn stale_session_guards_cannot_mutate_later_capture() {
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = Envelope {
        calls: 1,
        slots: 4,
        ..envelope()
    };
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation = MemoryConsumer::new("ownership generation control").register(&pool);
    let first = Session::create(&directory.path("first"), limits).unwrap();
    first.begin_call().unwrap();
    let mut old_owner = Life::new(Kind::Batch, 0, 0);
    let old_id = old_owner.id();
    let old_piece = Piece::new(&reservation, old_id);
    let old_transition = Transition::begin();
    let old_queue = QueueRetirement(old_id, old_owner.generation());
    let old_generation = old_owner.generation();
    let (second, current, piece) = scoped(old_id, old_piece.id(), || {
        drop(first);
        let second = Session::create(&directory.path("second"), limits).unwrap();
        second.begin_call().unwrap();
        let current = Life::new(Kind::Batch, 0, 0);
        assert!(current.id() > old_id);
        let piece = Piece::new(&reservation, current.id());
        (second, current, piece)
    });
    assert_eq!(origin(), (0, 0));
    current.update(|o| {
        o.piece = piece.id();
        o.bytes = 42;
    });
    let before = with(|r| {
        (
            r.constructed,
            r.retired,
            r.epoch,
            r.in_flight,
            r.operations,
            r.poison,
        )
    });
    old_owner.update(|o| o.bytes = 999);
    old_owner.raw(35, 123, 64, 64);
    old_owner.edge(current.id(), 1, 64);
    old_owner.begin_drop();
    old_piece.refresh(&reservation, current.id());
    drop(old_piece.child(&reservation, current.id(), 0));
    // An old reset scope must not publish a new session's borrowed bridge.
    assert!(
        update_thread_generation(old_generation, |t| t.origin = (old_id, 0)).is_none()
    );
    drop(old_owner);
    drop(old_piece);
    drop(old_transition);
    drop(old_queue);
    assert_eq!(
        before,
        with(|r| (
            r.constructed,
            r.retired,
            r.epoch,
            r.in_flight,
            r.operations,
            r.poison
        ))
    );
    assert_eq!(
        with(|r| r.owners.iter().flatten().next().unwrap().bytes),
        Some(42)
    );
    assert_eq!(
        with(|r| (
            r.backings.iter().flatten().count(),
            r.edges.iter().flatten().count(),
            r.pieces.iter().flatten().count()
        )),
        Some((0, 0, 1))
    );
    second.snapshot(0);
    drop(current);
    drop(piece);
    second.snapshot(0);
    second.end_call(0);
    second.finish().unwrap();

    // Successful finish also permits an escaped original owner. Its later Drop
    // cannot mutate the next committed capture's live owner.
    let third = Session::create(&directory.path("third"), limits).unwrap();
    third.begin_call().unwrap();
    let escaped = Life::new(Kind::Output, 0, 0);
    let escaped_id = escaped.id();
    third.snapshot(0);
    third.snapshot(0);
    third.end_call(0);
    third.finish().unwrap();
    let fourth = Session::create(&directory.path("fourth"), limits).unwrap();
    fourth.begin_call().unwrap();
    let retained = Life::new(Kind::Output, 0, 0);
    assert!(retained.id() > escaped_id);
    drop(escaped);
    assert_eq!(with(|r| (r.constructed, r.retired)), Some((1, 0)));
    fourth.snapshot(0);
    drop(retained);
    fourth.snapshot(0);
    fourth.end_call(0);
    fourth.finish().unwrap();
}

#[cfg_attr(test, test)]
fn foreign_constructor_and_queue_endpoints_fail_closed_without_id_reuse() {
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = Envelope {
        calls: 1,
        slots: 4,
        edges: 4,
        ..envelope()
    };
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation =
        MemoryConsumer::new("ownership foreign endpoint control").register(&pool);
    let first = Session::create(&directory.path("first"), limits).unwrap();
    first.begin_call().unwrap();
    let old_owner = Life::new(Kind::Queue, 0, 0);
    let old_id = old_owner.id();
    let old_piece = Piece::new(&reservation, old_id);
    let old_piece_id = old_piece.id();
    drop(first);
    let second = Session::create(&directory.path("second"), limits).unwrap();
    second.begin_call().unwrap();
    let current = Life::new(Kind::Queue, 0, 0);
    assert!(current.id() > old_piece_id);
    let piece = Piece::new(&reservation, current.id());
    assert_eq!(Life::new(Kind::Batch, old_id, 0).id(), 0);
    assert_eq!(Life::new(Kind::Batch, current.id(), old_piece_id).id(), 0);
    assert_eq!(Piece::new(&reservation, old_id).id(), 0);
    assert_eq!(
        scoped(old_id, old_piece_id, || Life::inherited(Kind::Batch)).id(),
        0
    );
    current.edge(old_id, 1, 16);
    Life::new(Kind::Batch, current.id(), 0).queued(old_id, 0);
    assert_eq!(
        with(|r| (
            r.owners.iter().flatten().count(),
            r.pieces.iter().flatten().count(),
            r.edges.iter().flatten().count()
        )),
        Some((1, 1, 0))
    );
    assert_eq!(with(|r| r.gaps), Some(GAP_UNKNOWN_SPONSOR));
    let exhausted = AtomicU64::new(u64::MAX);
    assert_eq!(next_identifier(&exhausted), None);
    assert_eq!(exhausted.load(Ordering::Relaxed), u64::MAX);
    second.snapshot(0);
    drop(current);
    drop(piece);
    drop(old_owner);
    drop(old_piece);
    second.snapshot(0);
    second.end_call(0);
    second.finish().unwrap();
}

#[cfg_attr(test, test)]
fn actual_split_keeps_donor_holder_and_lineage_until_transfer() {
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = Envelope {
        slots: 4,
        ..envelope()
    };
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    session.begin_call().unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation = MemoryConsumer::new("ownership split control").register(&pool);
    reservation.try_grow(128).unwrap();
    let donor = Life::new(Kind::Merge, 0, 0);
    let root = Piece::new(&reservation, donor.id());
    let split = reservation.split(64);
    let child = root.child(&split, donor.id(), 64);
    root.refresh(&reservation, donor.id());
    let child_id = child.id();
    assert_ne!(child_id, 0);
    let (credit, moved) = child.transfer_to(donor.id(), Kind::Credit as u64, || {
        let credit = Life::inherited(Kind::Credit);
        let moved = Piece::new(&split, credit.id());
        (credit, moved)
    });
    assert_eq!(moved.id(), child_id);
    assert_eq!(
        with(|r| {
            let piece = r
                .pieces
                .iter()
                .flatten()
                .find(|p| p.id == child_id)
                .unwrap();
            (
                piece.holder,
                piece.root,
                piece.split_from,
                piece.split_bytes,
                r.gaps,
            )
        }),
        Some((credit.id(), root.id(), root.id(), 64, 0))
    );
    session.snapshot(0);
    drop((moved, credit, split, root, donor, reservation));
    assert_eq!(pool.reserved(), 0);
    session.snapshot(0);
    session.end_call(0);
    complete_empty_call(&session);
    session.finish().unwrap();
    assert!(
        directory
            .lines("capture")
            .iter()
            .filter(|line| kind(line, "snapshot_end"))
            .all(|line| line.contains("\"complete\":true"))
    );
}

#[cfg_attr(test, test)]
fn construction_between_declared_calls_poison_capture_but_updates_and_drops_remain_legal()
{
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(
        &directory.path("capture"),
        Envelope {
            slots: 2,
            ..envelope()
        },
    )
    .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation =
        MemoryConsumer::new("ownership between-call control").register(&pool);
    session.begin_call().unwrap();
    let owner = Life::new(Kind::Queue, 0, 0);
    session.snapshot(0);
    session.snapshot(0);
    session.end_call(0);
    owner.update(|o| o.bytes = 7);
    assert_eq!(with(|r| r.poison), Some(0));
    assert_eq!(Life::new(Kind::Batch, owner.id(), 0).id(), 0);
    assert_eq!(Piece::new(&reservation, owner.id()).id(), 0);
    assert_eq!(
        with(|r| (r.poison, r.constructed)),
        Some((POISON_PROTOCOL, 1))
    );
    owner.update(|o| o.bytes = 9);
    drop(owner);
    assert_eq!(with(|r| r.retired), Some(1));
    session.begin_call().unwrap();
    session.snapshot(0);
    session.snapshot(0);
    session.end_call(0);
    assert!(session.finish().is_err());
    let cuts: Vec<_> = directory
        .lines("capture")
        .into_iter()
        .filter(|line| kind(line, "snapshot_begin"))
        .collect();
    assert!(
        cuts[2..]
            .iter()
            .all(|line| number(line, "poison") & POISON_PROTOCOL != 0)
    );
}

#[cfg_attr(test, test)]
fn unsupported_coverage_survives_retained_alias_until_final_retirement() {
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = Envelope {
        calls: 1,
        slots: 2,
        ..envelope()
    };
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    session.begin_call().unwrap();
    let unsupported = Life::new(Kind::Batch, 0, 0);
    unsupported.gap(GAP_UNSUPPORTED);
    let alias = Life::new(Kind::Output, unsupported.id(), 0);
    alias.edge(unsupported.id(), 3, 0);
    let transition = Transition::begin();
    drop(unsupported);
    drop(transition);
    assert_eq!(
        with(|r| r.owners.iter().flatten().next().unwrap().coverage),
        Some(GAP_UNSUPPORTED)
    );
    session.snapshot(0);
    drop(alias);
    session.snapshot(0);
    session.end_call(0);
    session.finish().unwrap();
    let cuts: Vec<_> = directory
        .lines("capture")
        .into_iter()
        .filter(|l| kind(l, "snapshot_begin"))
        .collect();
    assert_eq!(number(&cuts[0], "gaps"), GAP_UNSUPPORTED);
    assert_eq!(number(&cuts[1], "gaps"), 0);
}

#[cfg_attr(test, test)]
fn retirement_reuses_one_live_slot_across_many_generations() {
    let _guard = session_guard();
    let directory = Directory::new();
    let limits = envelope();
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    let mut previous = 0;
    const GENERATIONS_PER_CALL: u64 = 65_537;
    for _ in 0..2 {
        session.begin_call().unwrap();
        let before = with(|r| (r.records, r.bytes)).unwrap();
        let file_before = std::fs::metadata(directory.path("capture")).unwrap().len();
        for _ in 0..GENERATIONS_PER_CALL {
            let owner = Life::new(Kind::Source, 0, 0);
            assert!(owner.id() > previous);
            previous = owner.id();
            assert_eq!(with(|r| r.owners.iter().flatten().count()), Some(1));
            drop(owner);
            assert_eq!(with(|r| r.owners.iter().flatten().count()), Some(0));
        }
        // Construction and retirement churn perform no file I/O. Only cuts
        // and protocol boundaries can consume the fixed transport allowance.
        assert_eq!(with(|r| (r.records, r.bytes)), Some(before));
        assert_eq!(
            std::fs::metadata(directory.path("capture")).unwrap().len(),
            file_before
        );
        session.snapshot(0);
        session.snapshot(0);
        session.end_call(0);
    }
    assert_eq!(
        with(|r| (r.constructed, r.retired, r.poison)),
        Some((2 * GENERATIONS_PER_CALL, 2 * GENERATIONS_PER_CALL, 0))
    );
    session.finish().unwrap();
    let lines = directory.lines("capture");
    // Retired generations do not accumulate event records or retained owners.
    assert!(!lines.iter().any(|l| kind(l, "owner")));
    assert_eq!(lines.iter().filter(|l| kind(l, "snapshot_end")).count(), 4);
    assert_eq!(lines.len(), 12);
    assert!(lines.len() <= limits.bounds().unwrap().records);
    for line in lines.iter().filter(|l| kind(l, "snapshot_begin")) {
        let expected = number(line, "call") * GENERATIONS_PER_CALL;
        assert_eq!(number(line, "constructed"), expected);
        assert_eq!(number(line, "retired"), expected);
    }
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
        Envelope {
            depth: 17,
            ..envelope()
        },
        Envelope {
            columns: 9,
            ..envelope()
        },
        Envelope {
            nodes: 9,
            ..envelope()
        },
        Envelope {
            buffers: 9,
            ..envelope()
        },
        // Individually valid arena counts still cannot exceed the whole-file
        // record or byte allowance over the declared number of calls.
        Envelope {
            slots: 32_768,
            ..envelope()
        },
        Envelope {
            calls: 64,
            slots: 256,
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

#[cfg_attr(test, test)]
fn descriptor_arena_exact_fill_commits_and_one_more_backing_poisons() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use std::sync::Arc;

    let _guard = session_guard();
    let directory = Directory::new();
    let batch = RecordBatch::try_from_iter([(
        "number",
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
    )])
    .unwrap();
    let extra_storage = vec![0_u8; 8];
    for excess in [false, true] {
        let name = if excess { "overfull" } else { "full" };
        let limits = Envelope {
            calls: 1,
            descriptors: 2,
            nodes: 1,
            buffers: 1,
            ..envelope()
        };
        let session = Session::create(&directory.path(name), limits).unwrap();
        session.begin_call().unwrap();
        let owner = Life::new(Kind::Batch, 0, 0);
        owner.batch(&batch);
        assert_eq!(
            with(|r| (r.backings.iter().flatten().count(), r.poison, r.gaps)),
            Some((2, 0, 0))
        );
        if excess {
            owner.raw(
                36,
                extra_storage.as_ptr() as usize,
                extra_storage.capacity(),
                extra_storage.len(),
            );
            assert_eq!(with(|r| r.poison & POISON_CAPACITY), Some(POISON_CAPACITY));
            assert_eq!(with(|r| r.backings.iter().flatten().count()), Some(2));
        }
        session.snapshot(0);
        drop(owner);
        assert_eq!(with(|r| r.backings.iter().flatten().count()), Some(0));
        session.snapshot(0);
        session.end_call(0);
        assert_eq!(session.finish().is_err(), excess);
        let lines = directory.lines(name);
        assert_eq!(lines.iter().filter(|l| kind(l, "backing")).count(), 2);
        assert!(
            lines
                .iter()
                .filter(|l| kind(l, "snapshot_end"))
                .all(|l| l.contains(if excess {
                    "\"complete\":false"
                } else {
                    "\"complete\":true"
                }))
        );
    }
}

#[cfg_attr(test, test)]
fn edge_arena_updates_existing_edge_and_rejects_one_more_relation() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(
        &directory.path("capture"),
        Envelope {
            calls: 1,
            slots: 2,
            edges: 1,
            ..envelope()
        },
    )
    .unwrap();
    session.begin_call().unwrap();
    let from = Life::new(Kind::Merge, 0, 0);
    let to = Life::new(Kind::Key, 0, 0);
    from.edge(to.id(), 1, 8);
    from.edge(to.id(), 1, 16);
    assert_eq!(
        with(|r| (r.edges.iter().flatten().count(), r.poison)),
        Some((1, 0))
    );
    assert_eq!(
        with(|r| r.edges.iter().flatten().next().unwrap().bytes),
        Some(16)
    );
    from.edge(to.id(), 2, 32);
    assert_eq!(with(|r| r.poison & POISON_CAPACITY), Some(POISON_CAPACITY));
    assert_eq!(with(|r| r.edges.iter().flatten().count()), Some(1));
    session.snapshot(0);
    drop(from);
    drop(to);
    assert_eq!(with(|r| r.edges.iter().flatten().count()), Some(0));
    session.snapshot(0);
    session.end_call(0);
    assert!(session.finish().is_err());
    let lines = directory.lines("capture");
    let edges: Vec<_> = lines.iter().filter(|l| kind(l, "edge")).collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(number(edges[0], "bytes"), 16);
    assert_eq!(number(edges[0], "relation"), 1);
}

#[cfg_attr(test, test)]
fn real_lifetimes_and_transitions_at_counter_limit_cannot_commit() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(
        &directory.path("capture"),
        Envelope {
            calls: 1,
            ..envelope()
        },
    )
    .unwrap();
    session.begin_call().unwrap();
    with(|r| {
        r.next = u64::MAX - 1;
        r.constructed = u64::MAX - 1;
        r.retired = u64::MAX - 1;
        r.epoch = u64::MAX - 1;
        r.operations = u64::MAX - 1;
    })
    .unwrap();
    let first = Life::new(Kind::Source, 0, 0);
    let first_id = first.id();
    assert_ne!(first_id, 0);
    drop(first);
    assert_eq!(
        with(|r| (r.next, r.constructed, r.retired, r.poison)),
        Some((u64::MAX, u64::MAX, u64::MAX, 0))
    );
    let last = Life::new(Kind::Source, 0, 0);
    assert!(last.id() > first_id);
    drop(last);
    drop(Transition::begin());
    drop(Transition::begin());
    assert_eq!(
        with(|r| (
            r.next,
            r.constructed,
            r.retired,
            r.epoch,
            r.operations,
            r.in_flight
        )),
        Some((u64::MAX, u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0))
    );
    assert_eq!(
        with(|r| r.poison & POISON_ARITHMETIC),
        Some(POISON_ARITHMETIC)
    );
    session.snapshot(0);
    session.snapshot(0);
    session.end_call(0);
    assert!(session.finish().is_err());
    assert!(
        directory
            .lines("capture")
            .last()
            .unwrap()
            .contains("\"complete\":false")
    );
}

#[cfg_attr(test, test)]
fn full_live_arenas_leave_exactly_one_record_for_commit_footer() {
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;

    let _guard = session_guard();
    let directory = Directory::new();
    let limits = Envelope {
        slots: 2,
        descriptors: 2,
        edges: 1,
        nodes: 1,
        buffers: 1,
        ..envelope()
    };
    let bounds = limits.bounds().unwrap();
    let session = Session::create(&directory.path("capture"), limits).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let reservation = MemoryConsumer::new("ownership footer control").register(&pool);
    let batch = RecordBatch::try_from_iter([(
        "number",
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
    )])
    .unwrap();
    for _ in 0..2 {
        session.begin_call().unwrap();
        let first = Life::new(Kind::Merge, 0, 0);
        let second = Life::new(Kind::Batch, first.id(), 0);
        let first_piece = Piece::new(&reservation, first.id());
        let second_piece = Piece::new(&reservation, second.id());
        first.update(|o| o.piece = first_piece.id());
        second.update(|o| o.piece = second_piece.id());
        second.batch(&batch);
        first.edge(second.id(), 1, 16);
        assert_eq!(
            with(|r| (
                r.owners.iter().flatten().count(),
                r.pieces.iter().flatten().count(),
                r.backings.iter().flatten().count(),
                r.edges.iter().flatten().count(),
                r.poison,
                r.gaps,
            )),
            Some((2, 2, 2, 1, 0, 0))
        );
        session.snapshot(0);
        session.snapshot(0);
        // Both cuts observe full arenas. Retirement after the cuts prevents
        // retaining old generations while the next call fills the same slots.
        drop(first);
        drop(second);
        drop(first_piece);
        drop(second_piece);
        session.end_call(0);
    }
    assert_eq!(with(|r| r.records), Some(bounds.records as u64 - 1));
    let before = std::fs::metadata(directory.path("capture")).unwrap().len();
    assert!(before <= (bounds.bytes - LINE_BYTES) as u64);
    session.finish().unwrap();
    let lines = directory.lines("capture");
    assert_eq!(lines.len(), bounds.records);
    let footer = lines.last().unwrap();
    assert!(kind(footer, "session_end"));
    assert!(footer.contains("\"complete\":true"));
    assert_eq!(number(footer, "records"), bounds.records as u64 - 1);
    assert_eq!(number(footer, "bytes"), before);
    assert!(
        std::fs::metadata(directory.path("capture")).unwrap().len()
            <= bounds.bytes as u64
    );
}

#[cfg(target_os = "linux")]
#[cfg_attr(test, test)]
fn final_footer_write_failure_never_reports_committed_capture() {
    let _guard = session_guard();
    let directory = Directory::new();
    let session = Session::create(&directory.path("capture"), envelope()).unwrap();
    complete_empty_call(&session);
    complete_empty_call(&session);
    assert_eq!(with(|r| r.poison), Some(0));
    let full = OpenOptions::new().write(true).open("/dev/full").unwrap();
    with(|r| r.file = full).unwrap();
    assert!(session.finish().is_err());
    let lines = directory.lines("capture");
    assert_eq!(lines.iter().filter(|l| kind(l, "run_end")).count(), 2);
    assert!(!lines.iter().any(|l| kind(l, "session_end")));
}

/// Execute these controls from the existing workspace product test lane.
pub(super) fn run_controls() {
    sequential_calls_preserve_generation_and_counters_without_reset();
    actual_split_keeps_donor_holder_and_lineage_until_transfer();
    stale_session_guards_cannot_mutate_later_capture();
    foreign_constructor_and_queue_endpoints_fail_closed_without_id_reuse();
    construction_between_declared_calls_poison_capture_but_updates_and_drops_remain_legal(
    );
    unsupported_coverage_survives_retained_alias_until_final_retirement();
    retirement_reuses_one_live_slot_across_many_generations();
    capture_creation_is_exclusive_for_active_session_and_existing_file();
    dropping_session_leaves_no_commit_footer_and_releases_global_capture();
    invalid_zero_and_overflow_envelopes_are_rejected_before_file_creation();
    live_owner_overflow_poison_persists_after_owners_retire();
    snapshot_during_open_transition_is_explicitly_incomplete();
    calls_without_exactly_two_snapshots_cannot_commit();
    finishing_with_missing_or_open_call_cannot_commit();
    arithmetic_counter_overflow_poison_does_not_wrap_generation();
    descriptor_arena_exact_fill_commits_and_one_more_backing_poisons();
    edge_arena_updates_existing_edge_and_rejects_one_more_relation();
    real_lifetimes_and_transitions_at_counter_limit_cannot_commit();
    full_live_arenas_leave_exactly_one_record_for_commit_footer();
    #[cfg(target_os = "linux")]
    {
        file_write_failure_poison_prevents_commit();
        final_footer_write_failure_never_reports_committed_capture();
    }
}
