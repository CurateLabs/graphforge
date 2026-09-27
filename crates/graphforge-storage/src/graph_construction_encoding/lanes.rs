//! Bounded compression pipeline; durable writes and receipt ordering stay on the caller.
use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, storage,
    write_parquet, write_parquet_chunks,
};
use crate::graph_construction::cpu_admission::{ConstructionCpuAdmission, ConstructionCpuLease};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

type Compressed = Result<Vec<Vec<u8>>, GfError>;
struct Job {
    index: usize,
    path: String,
    batch: RecordBatch,
    cache_window: NonZeroU64,
}
struct Pool {
    sender: Option<mpsc::Sender<(usize, RecordBatch)>>,
    receiver: mpsc::Receiver<(usize, Compressed)>,
    workers: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    _lease: ConstructionCpuLease,
}
impl Pool {
    fn new(lease: ConstructionCpuLease) -> Self {
        let (sender, jobs) = mpsc::channel::<(usize, RecordBatch)>();
        let jobs = Arc::new(Mutex::new(jobs));
        let (results, receiver) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..lease.lanes().get())
            .map(|_| {
                let jobs = jobs.clone();
                let results = results.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    loop {
                        let job = jobs
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .recv();
                        let Ok((index, batch)) = job else {
                            break;
                        };
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            compress(&batch, &stop)
                        }))
                        .unwrap_or_else(|_| Err(storage("encoding lane panicked")));
                        if results.send((index, result)).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        Self {
            sender: Some(sender),
            receiver,
            workers,
            stop,
            _lease: lease,
        }
    }
    fn send(&self, index: usize, batch: RecordBatch) -> Result<(), GfError> {
        self.sender
            .as_ref()
            .expect("live pool")
            .send((index, batch))
            .map_err(storage)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub(super) struct ParquetLanes {
    pool: Option<Pool>,
    jobs: VecDeque<Job>,
    completed: BTreeMap<usize, Compressed>,
    undispatched: Vec<(usize, RecordBatch)>,
    next: usize,
    bytes: usize,
    budget: usize,
    reversed: bool,
}
impl ParquetLanes {
    pub(super) fn new(admission: Option<&Arc<ConstructionCpuAdmission>>, budget: usize) -> Self {
        let pool = admission
            .and_then(|admission| {
                admission.try_acquire(
                    NonZeroUsize::new(admission.limit().min(8)).expect("positive admission"),
                )
            })
            .filter(|lease| lease.lanes().get() > 1)
            .map(Pool::new);
        Self {
            pool,
            jobs: VecDeque::new(),
            completed: BTreeMap::new(),
            undispatched: Vec::new(),
            next: 0,
            bytes: 0,
            budget,
            reversed: crate::graph_construction::lane_jobs_reversed(),
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn push(
        &mut self,
        root: &StableDirectory,
        path: &str,
        batch: &RecordBatch,
        cache_window: NonZeroU64,
        evidence: &mut GraphConstructionEncodingEvidence,
        cancelled: &mut impl FnMut() -> bool,
        artifacts: &mut Vec<ConstructionEncodedArtifact>,
    ) -> Result<(), GfError> {
        let Some(pool) = &self.pool else {
            artifacts.push(write_parquet(
                root,
                path,
                batch,
                cache_window,
                evidence,
                cancelled,
            )?);
            return Ok(());
        };
        let width = pool.workers.len() + 1;
        let bytes = batch.get_array_memory_size();
        while !self.jobs.is_empty()
            && (self.jobs.len() >= width || bytes > self.budget.saturating_sub(self.bytes))
        {
            self.drain_one(root, evidence, cancelled, artifacts)?;
        }
        let index = self.next;
        self.next += 1;
        if self.reversed {
            self.undispatched.push((index, batch.clone()));
        } else {
            self.pool
                .as_ref()
                .expect("admitted pool")
                .send(index, batch.clone())?;
        }
        self.jobs.push_back(Job {
            index,
            path: path.to_owned(),
            batch: batch.clone(),
            cache_window,
        });
        self.bytes += bytes;
        Ok(())
    }
    fn drain_one(
        &mut self,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEncodingEvidence,
        cancelled: &mut impl FnMut() -> bool,
        artifacts: &mut Vec<ConstructionEncodedArtifact>,
    ) -> Result<(), GfError> {
        let pool = self.pool.as_ref().expect("queued work has pool");
        for (index, batch) in self.undispatched.drain(..).rev() {
            pool.send(index, batch)?;
        }
        let job = self.jobs.front().expect("queued job");
        while !self.completed.contains_key(&job.index) {
            if cancelled() {
                pool.stop.store(true, Ordering::Release);
                return Err(storage("construction encoding cancelled"));
            }
            match pool.receiver.recv_timeout(Duration::from_millis(5)) {
                Ok((index, result)) => {
                    self.completed.insert(index, result);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(storage("encoding lane disconnected"));
                }
            }
        }
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let job = self.jobs.pop_front().expect("queued job");
        self.bytes -= job.batch.get_array_memory_size();
        let chunks = self.completed.remove(&job.index).expect("completed job")?;
        artifacts.push(write_parquet_chunks(
            root,
            &job.path,
            &job.batch,
            job.cache_window,
            evidence,
            cancelled,
            Some(chunks),
        )?);
        Ok(())
    }
    pub(super) fn flush(
        &mut self,
        root: &StableDirectory,
        evidence: &mut GraphConstructionEncodingEvidence,
        cancelled: &mut impl FnMut() -> bool,
        artifacts: &mut Vec<ConstructionEncodedArtifact>,
    ) -> Result<(), GfError> {
        while !self.jobs.is_empty() {
            self.drain_one(root, evidence, cancelled, artifacts)?;
        }
        Ok(())
    }
}

/// Preserve every Parquet write boundary when replaying onto the durable sink,
/// including cache-window and I/O evidence. No worker publishes a file.
struct Chunks<'a> {
    writes: Vec<Vec<u8>>,
    stop: &'a AtomicBool,
}
impl Write for Chunks<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.stop.load(Ordering::Acquire) {
            return Err(std::io::Error::other("construction encoding cancelled"));
        }
        self.writes.push(bytes.to_vec());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn compress(batch: &RecordBatch, stop: &AtomicBool) -> Result<Vec<Vec<u8>>, GfError> {
    let mut writer = ArrowWriter::try_new(
        Chunks {
            writes: Vec::new(),
            stop,
        },
        batch.schema(),
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    Ok(writer.into_inner().map_err(storage)?.writes)
}
