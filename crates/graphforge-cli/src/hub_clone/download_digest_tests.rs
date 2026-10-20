//! The package is hashed once, as it downloads, and every later reader is held
//! to that digest instead of the package being read back to recompute it
//! (#1770). Reads are counted by the kernel's per-thread `rchar`
//! (`/proc/thread-self/io`), so the tests run on Linux, and tests running on
//! other threads of the same process cannot add to a count. Everything a test
//! measures runs on its own thread.
#![cfg(target_os = "linux")]

use super::tests::{Scripted, bundle_of_nodes, clone_documents, object, response};
use super::transfer_tests::clone_args;
use super::*;
use std::sync::{Arc, Mutex};

/// Bytes this thread has asked the kernel to read, files and sockets alike.
fn bytes_read() -> u64 {
    std::fs::read_to_string("/proc/thread-self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// Control files (the resume checkpoint, small JSON documents) a phase may
/// read besides the package.
const CONTROL_READ_SLACK: u64 = 16 * 1024;

fn fresh_partial(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, ObjectDescriptor) {
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let mut descriptor = object(bytes);
    descriptor.locations = vec!["https://objects.example/project.gfpb".into()];
    (root, partial, descriptor)
}

/// The checkpoint a download that stopped part way leaves beside its partial.
fn write_checkpoint(partial: &Path, descriptor: &ObjectDescriptor) {
    std::fs::write(
        partial.with_extension("resume.json"),
        serde_json::to_vec(&serde_json::json!({
            "digest": descriptor.digest.0,
            "length": descriptor.length,
            "location": descriptor.locations[0],
            "etag": "\"fixture-1\"",
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn a_fresh_download_is_hashed_as_it_arrives_and_never_read_back() {
    let (bundle, _) = bundle_of_nodes(20_000);
    let length = bundle.len() as u64;
    assert!(length > 8 * CONTROL_READ_SLACK, "{length}");
    let (_root, partial, descriptor) = fresh_partial(&bundle);
    let transport = Scripted::new(vec![response(200, None, &bundle)]);
    let before = bytes_read();
    let (report, digest) = download_with_progress(
        &transport,
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&TEST_RETRY_POLICY),
    )
    .unwrap();
    let read = bytes_read() - before;
    println!("FRESH length={length} read_back={read}");
    assert_eq!(report.transferred_bytes, length);
    assert_eq!(digest, descriptor.digest.0);
    assert_eq!(std::fs::read(&partial).unwrap(), bundle);
    assert!(
        read <= CONTROL_READ_SLACK,
        "a fresh download read {read} bytes back from a {length}-byte package"
    );
}

#[test]
fn a_resumed_download_reads_only_the_prefix_it_resumes_from() {
    let (bundle, _) = bundle_of_nodes(20_000);
    let length = bundle.len() as u64;
    let prefix = length / 2;
    let (_root, partial, descriptor) = fresh_partial(&bundle);
    std::fs::write(&partial, &bundle[..prefix as usize]).unwrap();
    write_checkpoint(&partial, &descriptor);
    let range = format!("bytes {prefix}-{}/{length}", length - 1);
    let transport = Scripted::new(vec![response(
        206,
        Some(&range),
        &bundle[prefix as usize..],
    )]);
    let before = bytes_read();
    let (report, digest) = download_with_progress(
        &transport,
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&TEST_RETRY_POLICY),
    )
    .unwrap();
    let read = bytes_read() - before;
    println!("RESUMED length={length} prefix={prefix} read={read}");
    assert_eq!(report.resumed_bytes, prefix);
    assert_eq!(digest, descriptor.digest.0);
    assert!(
        (prefix..=prefix + CONTROL_READ_SLACK).contains(&read),
        "resuming at {prefix} of {length} bytes read {read} bytes"
    );
}

#[test]
fn a_partial_that_is_already_complete_is_hashed_in_one_pass() {
    let (bundle, _) = bundle_of_nodes(20_000);
    let length = bundle.len() as u64;
    let (_root, partial, descriptor) = fresh_partial(&bundle);
    std::fs::write(&partial, &bundle).unwrap();
    write_checkpoint(&partial, &descriptor);
    let before = bytes_read();
    let (_, digest) = download_with_progress(
        &Scripted::new(Vec::new()),
        &descriptor,
        &partial,
        &mut DownloadReport::default(),
        &mut DownloadControl::quiet(&TEST_RETRY_POLICY),
    )
    .unwrap();
    let read = bytes_read() - before;
    assert_eq!(digest, descriptor.digest.0);
    assert!(
        (length..=length + CONTROL_READ_SLACK).contains(&read),
        "hashing a complete {length}-byte partial read {read} bytes"
    );
}

type Marks = Arc<Mutex<Vec<(&'static str, u64)>>>;

fn mark(marks: &Marks, label: &'static str) {
    marks.lock().unwrap().push((label, bytes_read()));
}

/// Records the process's read count when the package body ends.
struct MarkAtEnd {
    inner: Box<dyn Read + Send>,
    marks: Marks,
    ended: bool,
}

impl Read for MarkAtEnd {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        if read == 0 && !self.ended {
            self.ended = true;
            mark(&self.marks, "transfer_end");
        }
        Ok(read)
    }
}

struct PhaseTransport {
    inner: Scripted,
    marks: Marks,
}

impl Transport for PhaseTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        let mut response = self.inner.get(url, range, if_range, limit)?;
        if url.path().ends_with(".gfpb") {
            response.body = Box::new(MarkAtEnd {
                inner: response.body,
                marks: Arc::clone(&self.marks),
                ended: false,
            });
        }
        Ok(response)
    }
}

#[test]
fn between_the_download_and_the_import_the_package_is_verified_and_nothing_more() {
    let (bundle, package_digest) = bundle_of_nodes(20_000);
    let length = bundle.len() as u64;
    // What one full verification of this package reads, measured on its own.
    // The verifier's cost belongs to the verifier (it has grown before, and
    // will again); the clone must add nothing to it.
    let alone = tempfile::tempdir().unwrap();
    let path = alone.path().join("package.gfpb");
    std::fs::write(&path, &bundle).unwrap();
    let before = bytes_read();
    graphforge_storage::verify_portable(
        &path,
        graphforge_api::PortableMode::Full,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    let verifier_alone = bytes_read() - before;
    assert!(verifier_alone >= length, "{verifier_alone}");
    let marks = Marks::default();
    let (refs, manifest) = clone_documents(&bundle, &package_digest, &[]);
    let transport = PhaseTransport {
        inner: Scripted::new(vec![
            response(200, None, &refs),
            response(200, None, &manifest),
            response(200, None, &bundle),
        ]),
        marks: Arc::clone(&marks),
    };
    let import_marks = Arc::clone(&marks);
    let delays = CloneDelays {
        before_import: Some(Box::new(move || mark(&import_marks, "import_start"))),
        ..CloneDelays::default()
    };
    let root = tempfile::tempdir().unwrap();
    run_clone_profiled_with_delays(
        &transport,
        clone_args(&root.path().join("openalex")),
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &delays,
        &mut CloneEnv::quiet(),
    )
    .unwrap();
    mark(&marks, "end");
    let marks = marks.lock().unwrap();
    let at = |label: &str| marks.iter().find(|(name, _)| *name == label).unwrap().1;
    // The transfer ends with every byte hashed; what follows, before the
    // import starts, is one verification and no digest pass.
    let verification = at("import_start") - at("transfer_end");
    let import = at("end") - at("import_start");
    println!(
        "PHASES length={length} verifier_alone={verifier_alone} verification={verification} import={import}"
    );
    assert!(
        (verifier_alone..=verifier_alone + CONTROL_READ_SLACK).contains(&verification),
        "{verification} bytes were read between the download and the import of a \
         {length}-byte package whose verification alone reads {verifier_alone}"
    );
    assert!(import >= length, "the import reads the package: {import}");
}

#[test]
fn reads_on_other_threads_are_not_counted_against_the_measured_work() {
    let noise = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(noise.path(), vec![7_u8; 4 << 20]).unwrap();
    let before = bytes_read();
    let path = noise.path().to_path_buf();
    std::thread::spawn(move || std::fs::read(path).unwrap().len())
        .join()
        .unwrap();
    let counted = bytes_read() - before;
    assert!(
        counted < CONTROL_READ_SLACK,
        "a read on another thread added {counted} bytes to this thread's count"
    );
}
