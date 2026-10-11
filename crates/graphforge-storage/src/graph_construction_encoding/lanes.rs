//! Bounded compression pipeline; durable writes and receipt ordering stay on the caller.
#[cfg(any(test, feature = "test-support"))]
use super::seam_spike;
use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, storage,
    write_parquet,
};
use super::{
    CountingWriter, EncodingTempGuard, IoCounter, account_cache_release, add_evidence_counter,
    directory_for, hex,
};
use crate::graph_construction::cpu_admission::{ConstructionCpuAdmission, ConstructionCpuLease};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use graphforge_filesystem::file_identity;
use parquet::arrow::ArrowWriter;
use sha2::Digest;
use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsStr;
use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use uuid::Uuid;

type Compressed = Result<Vec<Vec<u8>>, GfError>;
struct Job {
    index: usize,
    path: String,
    batch: RecordBatch,
    cache_window: NonZeroU64,
}
struct EncodingJob {
    index: usize,
    batch: RecordBatch,
    #[cfg(any(test, feature = "test-support"))]
    digest_context: graphforge_core::hash_observation::operation::Context,
    lifecycle_context: crate::lifecycle_io::CaptureContext,
}

struct Pool {
    sender: Option<mpsc::Sender<EncodingJob>>,
    receiver: mpsc::Receiver<(usize, Compressed)>,
    workers: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    _lease: ConstructionCpuLease,
}
impl Pool {
    fn new(lease: ConstructionCpuLease) -> Self {
        let (sender, jobs) = mpsc::channel::<EncodingJob>();
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
                        let Ok(job) = job else {
                            break;
                        };
                        #[cfg(any(test, feature = "test-support"))]
                        let _digest_guard = job.digest_context.attach();
                        let _lifecycle_capture = job.lifecycle_context.attach();
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            compress(&job.batch, &stop)
                        }))
                        .unwrap_or_else(|_| Err(storage("encoding lane panicked")));
                        if results.send((job.index, result)).is_err() {
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
            .send(EncodingJob {
                index,
                batch,
                #[cfg(any(test, feature = "test-support"))]
                digest_context: graphforge_core::hash_observation::operation::Context::capture(),
                lifecycle_context: crate::lifecycle_io::CaptureContext::current(),
            })
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
                // Every lane the admission has free (#1863); the byte budget,
                // not the lane count, bounds batches in flight.
                admission
                    .try_acquire(NonZeroUsize::new(admission.limit()).expect("positive admission"))
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
            artifacts.extend(write_parquet(
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
        artifacts.extend(write_parquet_chunks(
            root,
            &job.path,
            &job.batch,
            job.cache_window,
            evidence,
            cancelled,
            Some(Encoded::Chunks(chunks)),
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

pub(super) enum Encoded {
    Chunks(Vec<Vec<u8>>),
    Object(bytes::Bytes),
}

fn is_property_object(relative: &str) -> bool {
    relative.starts_with("properties/") || relative.starts_with("edge_properties/")
}

/// Streams the chunks of one logical Parquet stream as a single `Read`.
struct ChunkReader<'a> {
    chunks: std::slice::Iter<'a, Vec<u8>>,
    current: &'a [u8],
}
impl std::io::Read for ChunkReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            match self.chunks.next() {
                Some(next) => self.current = next,
                None => return Ok(0),
            }
        }
        let count = self.current.len().min(buffer.len());
        buffer[..count].copy_from_slice(&self.current[..count]);
        self.current = &self.current[count..];
        Ok(count)
    }
}

/// Publish a property stream longer than one physical object as consecutive
/// bounded objects, each written once to its own staged name. Every object's
/// header repeats the stream's length and part count, so the stream is encoded
/// in memory first; no private logical copy of it reaches the disk.
#[allow(clippy::too_many_arguments)]
fn write_bounded_property_objects(
    root: &StableDirectory,
    relative: &str,
    batch: &RecordBatch,
    cache_window: NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    logical: &[Vec<u8>],
    length: u64,
) -> Result<Vec<ConstructionEncodedArtifact>, GfError> {
    let mut input = ChunkReader {
        chunks: logical.iter(),
        current: &[],
    };
    let mut artifacts = Vec::new();
    crate::property_overlay::bounded_object::encode_parts(&mut input, length, |index, bytes| {
        let path = crate::property_overlay::bounded_object::part_path(Path::new(relative), index);
        let path = path
            .to_str()
            .ok_or_else(|| storage("property object path is not UTF-8"))?
            .replace('\\', "/");
        artifacts.extend(write_parquet_chunks(
            root,
            &path,
            batch,
            cache_window,
            evidence,
            cancelled,
            Some(Encoded::Object(bytes)),
        )?);
        Ok(())
    })?;
    Ok(artifacts)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn write_parquet_chunks(
    root: &StableDirectory,
    relative: &str,
    batch: &RecordBatch,
    cache_window: std::num::NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    chunks: Option<Encoded>,
) -> Result<Vec<ConstructionEncodedArtifact>, GfError> {
    #[cfg(not(any(test, feature = "test-support")))]
    let _ = cancelled;
    let chunks = match chunks {
        // A property stream is bounded into physical objects whose headers
        // repeat the stream's whole length, so the stream is complete before
        // its first byte is written (see `write_bounded_property_objects`).
        None if is_property_object(relative) => {
            let stop = AtomicBool::new(false);
            let encoded = compress(batch, &stop)?;
            if cancelled() {
                return Err(storage("construction encoding cancelled"));
            }
            Some(Encoded::Chunks(encoded))
        }
        other => other,
    };
    if is_property_object(relative)
        && let Some(Encoded::Chunks(logical)) = &chunks
    {
        let length = logical.iter().map(|chunk| chunk.len() as u64).sum::<u64>();
        if length > crate::property_overlay::bounded_object::MAX_PROPERTY_OBJECT_BYTES as u64 {
            return write_bounded_property_objects(
                root,
                relative,
                batch,
                cache_window,
                evidence,
                cancelled,
                logical,
                length,
            );
        }
    }
    let (directory, name) = directory_for(root, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut temporary_guard = EncodingTempGuard {
        directory: &directory,
        name: temporary.clone(),
        identity,
        armed: true,
    };
    let counter = IoCounter::default();
    let sink = CountingWriter {
        inner: graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(file, cache_window)
            .map_err(storage)?,
        counter: counter.clone(),
        digest: Sha256::new(),
        checksum: crate::corruption_checksum::Checksum::new(),
    };
    let mut writer = if let Some(encoded) = chunks {
        let mut sink = sink;
        match encoded {
            Encoded::Chunks(chunks) => {
                for chunk in chunks {
                    if cancelled() {
                        return Err(storage("construction encoding cancelled"));
                    }
                    sink.write_all(&chunk).map_err(storage)?;
                }
            }
            Encoded::Object(bytes) => {
                if cancelled() {
                    return Err(storage("construction encoding cancelled"));
                }
                sink.write_all(&bytes).map_err(storage)?;
            }
        }
        sink
    } else {
        let mut writer = ArrowWriter::try_new(
            sink,
            batch.schema(),
            Some(crate::permanent_parquet::writer_properties().build()),
        )
        .map_err(storage)?;
        #[cfg(any(test, feature = "test-support"))]
        let writer = if seam_spike::enabled()? {
            seam_spike::write(batch, writer, cancelled)?
        } else {
            writer.write(batch).map_err(storage)?;
            writer.into_inner().map_err(storage)?
        };
        #[cfg(not(any(test, feature = "test-support")))]
        let writer = {
            writer.write(batch).map_err(storage)?;
            writer.into_inner().map_err(storage)?
        };
        writer
    };
    directory
        .seal_cache_writer(&mut writer.inner)
        .map_err(storage)?;
    let cache_release = writer.inner.evidence();
    account_cache_release(cache_release, evidence)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.parquet.after_temp_fsync.{relative}"
    ));
    let (written, operations) = counter.values();
    add_evidence_counter(
        &mut evidence.output_write_bytes,
        written,
        "output write bytes",
    )?;
    add_evidence_counter(
        &mut evidence.output_write_operations,
        operations,
        "output write operations",
    )?;
    add_evidence_counter(
        &mut evidence.fsync_operations,
        cache_release.sync_operations,
        "file fsync operations",
    )?;
    let artifact = ConstructionEncodedArtifact {
        path: relative.to_owned(),
        bytes: written,
        sha256: hex(&writer.digest.finalize()),
        xxh64: writer.checksum.finish(),
    };
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    temporary_guard.disarm();
    directory.acknowledge().map_err(storage)?;
    crate::graph_construction::construction_failpoint(&format!(
        "encode.parquet.after_install.{relative}"
    ));
    add_evidence_counter(
        &mut evidence.fsync_operations,
        1,
        "namespace fsync operations",
    )?;
    crate::graph_construction::diagnostics::sealed_payload(written, 2);
    Ok(vec![artifact])
}
