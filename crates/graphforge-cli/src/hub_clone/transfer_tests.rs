//! Large-transfer robustness: resumable downloads over the real HTTP read
//! transport, staging and atomic installation, preflight, and kill-and-rerun.

use super::tests::{
    LoopbackTransport, Scripted, clone_documents, clone_script, object, real_bundle, response,
};
use super::*;
use std::collections::BTreeSet;
use std::io::{BufRead as _, BufReader};
use std::net::{TcpListener, TcpStream};
use std::process::Command;

/// Serve one scripted handler per accepted connection, each on its own thread,
/// after reading the request head; return the address and the request heads.
fn serve(
    handlers: Vec<Box<dyn FnOnce(TcpStream, &str) + Send>>,
) -> (std::net::SocketAddr, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let mut workers = Vec::new();
        for handler in handlers {
            let (stream, _) = listener.accept().unwrap();
            workers.push(std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    request.push_str(&line.to_ascii_lowercase());
                }
                handler(stream, &request);
                request
            }));
        }
        // Dropping the listener refuses any further connection.
        drop(listener);
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    });
    (address, server)
}

fn head(stream: &mut TcpStream, status: &str, length: usize, extra: &str) {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nETag: \"fixture-1\"\r\n{extra}Connection: close\r\n\r\n"
    )
    .unwrap();
}

fn loopback_object(bytes: &[u8], address: std::net::SocketAddr) -> ObjectDescriptor {
    let mut descriptor = object(bytes);
    descriptor.locations = vec![format!("http://{address}/project.gfpb")];
    descriptor
}

fn payload() -> Vec<u8> {
    (0..1000_u32).map(|value| (value % 251) as u8).collect()
}

#[test]
fn full_body_fallback_after_an_ignored_range_is_read_to_the_object_length() {
    let bytes = payload();
    let descriptor = object(&bytes);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    std::fs::write(&partial, &bytes[..100]).unwrap();
    std::fs::write(
        partial.with_extension("resume.json"),
        serde_json::to_vec(&ResumeState {
            digest: descriptor.digest.0.clone(),
            length: bytes.len() as u64,
            location: descriptor.locations[0].clone(),
            etag: "\"fixture-1\"".into(),
        })
        .unwrap(),
    )
    .unwrap();
    // The server ignores `Range` and answers with the whole object.
    let transport = Scripted::new(vec![response(200, None, &bytes)]);
    assert_eq!(
        download(&transport, &descriptor, &partial).unwrap(),
        DownloadReport {
            resumed_bytes: 0,
            transferred_bytes: bytes.len() as u64,
            attempts: 1,
        }
    );
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);
}

#[test]
fn real_http_server_ignoring_range_yields_a_verified_object() {
    let bytes = payload();
    let first = bytes.clone();
    let second = bytes.clone();
    let (address, server) = serve(vec![
        Box::new(move |mut stream, _| {
            head(&mut stream, "200 OK", first.len(), "");
            stream.write_all(&first[..100]).unwrap();
        }),
        Box::new(move |mut stream, request| {
            assert!(request.contains("range: bytes=100-"), "{request}");
            head(&mut stream, "200 OK", second.len(), "");
            stream.write_all(&second).unwrap();
        }),
    ]);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let report = download(
        &LoopbackTransport::new(),
        &loopback_object(&bytes, address),
        &partial,
    )
    .unwrap();
    assert_eq!(report.attempts, 2);
    assert_eq!(report.resumed_bytes, 0);
    assert_eq!(report.transferred_bytes, 1100);
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);
    server.join().unwrap();
}

#[test]
fn real_http_download_outliving_the_idle_window_completes_in_one_request() {
    let bytes = payload();
    let served = bytes.clone();
    let (address, server) = serve(vec![Box::new(move |mut stream, _| {
        head(&mut stream, "200 OK", served.len(), "");
        for chunk in served.chunks(100) {
            stream.write_all(chunk).unwrap();
            stream.flush().unwrap();
            std::thread::sleep(Duration::from_millis(150));
        }
    })]);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let idle = Duration::from_millis(500);
    let started = Instant::now();
    let report = download(
        &LoopbackTransport::with_idle(idle),
        &loopback_object(&bytes, address),
        &partial,
    )
    .unwrap();
    // The body took three idle windows, yet one request carried all of it.
    assert!(started.elapsed() > idle * 2, "{:?}", started.elapsed());
    assert_eq!(report.attempts, 1);
    assert_eq!(report.transferred_bytes, bytes.len() as u64);
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);
    server.join().unwrap();
}

#[test]
fn real_http_stalled_body_resumes_in_process_with_range() {
    let bytes = payload();
    let first = bytes.clone();
    let second = bytes.clone();
    let stall = Duration::from_secs(4);
    let (address, server) = serve(vec![
        Box::new(move |mut stream, _| {
            head(&mut stream, "200 OK", first.len(), "");
            stream.write_all(&first[..100]).unwrap();
            stream.flush().unwrap();
            // Hold the connection open without sending anything.
            std::thread::sleep(stall);
        }),
        Box::new(move |mut stream, request| {
            assert!(request.contains("range: bytes=100-"), "{request}");
            assert!(request.contains("if-range: \"fixture-1\""), "{request}");
            head(
                &mut stream,
                "206 Partial Content",
                second.len() - 100,
                &format!("Content-Range: bytes 100-999/{}\r\n", second.len()),
            );
            stream.write_all(&second[100..]).unwrap();
        }),
    ]);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    let started = Instant::now();
    let report = download(
        &LoopbackTransport::with_idle(Duration::from_millis(300)),
        &loopback_object(&bytes, address),
        &partial,
    )
    .unwrap();
    // The idle bound, not the server closing the stalled connection, ended
    // the first request.
    assert!(started.elapsed() < stall / 2, "{:?}", started.elapsed());
    assert_eq!(report.attempts, 2);
    assert_eq!(report.transferred_bytes, bytes.len() as u64);
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);
    server.join().unwrap();
}

#[test]
fn transient_status_is_retried_and_a_permanent_one_is_not() {
    let start = Url::parse("https://hub.example/repository/.gf/refs").unwrap();
    let transport = Scripted::new(vec![response(503, None, b""), response(200, None, b"ok")]);
    let mut attempts = 0;
    let fetched = fetch_with_attempts(
        &transport,
        &start,
        None,
        None,
        1024,
        &mut attempts,
        &TEST_RETRY_POLICY,
    )
    .unwrap();
    assert_eq!(fetched.status, 200);
    assert_eq!(attempts, 2);

    let transport = Scripted::new(vec![response(404, None, b""), response(200, None, b"ok")]);
    let mut attempts = 0;
    let Err(error) = fetch_with_attempts(
        &transport,
        &start,
        None,
        None,
        1024,
        &mut attempts,
        &TEST_RETRY_POLICY,
    ) else {
        panic!("a missing document is not retried");
    };
    assert!(error.to_string().contains("HTTP status 404"), "{error}");
    assert_eq!(transport.remaining(), 1);
}

#[test]
fn retry_backoff_doubles_up_to_its_bound() {
    let waits: Vec<_> = (1..=7)
        .map(|failure| RETRY_POLICY.backoff(failure))
        .collect();
    assert_eq!(
        waits,
        [1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs).to_vec()
    );
}

#[test]
fn the_object_bound_is_the_publish_cumulative_bound() {
    assert_eq!(
        MAX_OBJECT_BYTES,
        DiscoveryLimits::default().max_cumulative_object_bytes
    );
}

fn clone_args(destination: &Path) -> CloneArgs {
    CloneArgs {
        repository: "openalex/openalex".into(),
        destination: Some(destination.to_path_buf()),
        telemetry_endpoint: None,
        git_ref: None,
        version_uuid: None,
    }
}

fn names(path: &Path) -> BTreeSet<String> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

fn clone_json(transport: &dyn Transport, destination: &Path) -> serde_json::Value {
    let mut output = Vec::new();
    run_clone_with(transport, clone_args(destination), true, &mut output).unwrap();
    serde_json::from_slice(&output).unwrap()
}

fn assert_reopens_at(destination: &Path, result: &serde_json::Value) {
    let graph = GraphForge::new(destination.to_str()).expect("clone reopens");
    assert_eq!(
        graph
            .committed_generation_identity()
            .unwrap()
            .generation_uuid
            .to_string(),
        result["generation_uuid"].as_str().unwrap()
    );
}

#[test]
fn objects_that_each_fit_clone_when_their_total_exceeds_one_hundred_gib() {
    let (bundle, package_digest) = real_bundle();
    let large = 101 * 1024 * 1024 * 1024_u64;
    let (refs, manifest) = clone_documents(
        &bundle,
        &package_digest,
        &[serde_json::json!({
            "digest": format!("sha256:{}", "f".repeat(64)),
            "length": large,
            "media_type": graphforge_discovery::PORTABLE_V2_MEDIA_TYPE,
            "locations": ["https://objects.example/other.gfpb"]
        })],
    );
    let transport = Scripted::new(vec![
        response(200, None, &refs),
        response(200, None, &manifest),
        response(200, None, &bundle),
    ]);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let result = clone_json(&transport, &destination);
    assert_eq!(result["package_digest"], package_digest);
    assert_reopens_at(&destination, &result);
}

#[test]
fn clone_installs_atomically_and_leaves_only_the_destination() {
    let (bundle, package_digest) = real_bundle();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let result = clone_json(&clone_script(&bundle, &package_digest), &destination);
    // No staging, import residue, probe, or admission lock is left beside it.
    assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
    assert_reopens_at(&destination, &result);
}

#[test]
fn a_staged_import_for_another_operation_is_replaced() {
    let (bundle, package_digest) = real_bundle();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let staging = staging_path(&destination).unwrap();
    std::fs::create_dir(&staging).unwrap();
    std::fs::create_dir(staging.join("project")).unwrap();
    std::fs::write(staging.join("project/foreign"), b"older version").unwrap();
    std::fs::write(staging.join(".project.portable-v2-older"), b"residue").unwrap();
    std::fs::write(
        staging.join("operation"),
        uuid::Uuid::from_u128(7).hyphenated().to_string(),
    )
    .unwrap();
    let result = clone_json(&clone_script(&bundle, &package_digest), &destination);
    assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
    assert_reopens_at(&destination, &result);
}

#[test]
fn an_inadmissible_destination_fails_before_any_request_with_its_real_code() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("hop")).unwrap();
    let destination = root.path().join("hop").join("..").join("openalex");
    let transport = Scripted::new(vec![response(200, None, b"never read")]);
    let mut output = Vec::new();
    let error =
        run_clone_with(&transport, clone_args(&destination), true, &mut output).unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM", "{error}");
    assert_eq!(transport.remaining(), 1, "nothing was requested");
    assert_eq!(names(root.path()), BTreeSet::from(["hop".to_owned()]));
}

#[cfg(target_os = "linux")]
#[test]
fn a_tmpfs_destination_fails_before_any_request_with_its_real_code() {
    let Ok(root) = tempfile::tempdir_in("/dev/shm") else {
        panic!("Linux provides /dev/shm");
    };
    let transport = Scripted::new(vec![response(200, None, b"never read")]);
    let error = run_clone_with(
        &transport,
        clone_args(&root.path().join("openalex")),
        true,
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM", "{error}");
    assert_eq!(transport.remaining(), 1, "nothing was requested");
    assert!(names(root.path()).is_empty());
}

#[test]
fn insufficient_space_fails_before_download_and_leaves_nothing() {
    let (bundle, package_digest) = real_bundle();
    let transport = clone_script(&bundle, &package_digest);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let mut env = CloneEnv {
        available_space: |_| Ok(1024),
        ..CloneEnv::quiet()
    };
    let error = run_clone_profiled_with_delays(
        &transport,
        clone_args(&destination),
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &CloneDelays::default(),
        &mut env,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("hub.insufficient_space"),
        "{error}"
    );
    assert_eq!(transport.remaining(), 1, "the object was never requested");
    assert!(names(root.path()).is_empty());
}

#[test]
fn required_space_covers_the_download_and_the_import_peak() {
    assert_eq!(required_space(1000, 0, 0, false), 1000 + 2250);
    assert_eq!(required_space(1000, 400, 0, false), 600 + 2250);
    assert_eq!(required_space(1000, 1000, 2000, false), 250);
    assert_eq!(required_space(1000, 1000, 5000, false), 0);
    assert_eq!(required_space(1000, 1000, 0, true), 1000);
    assert_eq!(required_space(1000, 1000, 600, true), 400);
    assert_eq!(required_space(u64::MAX, 0, 0, false), u64::MAX);
}

#[test]
fn cancellation_reaches_the_import_and_the_rerun_completes() {
    static CANCEL: AtomicBool = AtomicBool::new(false);
    let (bundle, package_digest) = real_bundle();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let mut env = CloneEnv {
        cancelled: &CANCEL,
        ..CloneEnv::quiet()
    };
    let delays = CloneDelays {
        before_import: Some(Box::new(|| CANCEL.store(true, Ordering::SeqCst))),
        ..CloneDelays::default()
    };
    let error = run_clone_profiled_with_delays(
        &clone_script(&bundle, &package_digest),
        clone_args(&destination),
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &delays,
        &mut env,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("hub.package.cancelled"),
        "{error}"
    );
    assert!(!destination.exists());
    CANCEL.store(false, Ordering::SeqCst);
    let result = clone_json(&clone_script(&bundle, &package_digest), &destination);
    assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
    assert_reopens_at(&destination, &result);
}

#[test]
fn progress_reports_bytes_and_phases() {
    let (bundle, package_digest) = real_bundle();
    let root = tempfile::tempdir().unwrap();
    let mut lines = Vec::new();
    let mut env = CloneEnv {
        progress: CloneProgress::new(Some(&mut lines)),
        ..CloneEnv::quiet()
    };
    run_clone_profiled_with_delays(
        &clone_script(&bundle, &package_digest),
        clone_args(&root.path().join("openalex")),
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &CloneDelays::default(),
        &mut env,
    )
    .unwrap();
    drop(env);
    let text = String::from_utf8(lines).unwrap();
    let total = human_bytes(bundle.len() as u64);
    assert!(
        text.contains(&format!("downloaded {total} of {total} (100%)")),
        "{text}"
    );
    assert!(text.contains("gf clone: verifying the package"), "{text}");
    assert!(text.contains("gf clone: importing the project"), "{text}");
}

const HELPER: &str = "hub_clone::transfer_tests::subprocess_clone_helper";

/// Clone from the fixture named by the environment; a no-op otherwise. The
/// kill-and-rerun test runs it in a child process with a failpoint set.
#[test]
fn subprocess_clone_helper() {
    let Ok(fixture) = std::env::var("GRAPHFORGE_CLONE_HELPER_FIXTURE") else {
        return;
    };
    let fixture = PathBuf::from(fixture);
    let destination = PathBuf::from(std::env::var("GRAPHFORGE_CLONE_HELPER_DESTINATION").unwrap());
    let read = |name: &str| std::fs::read(fixture.join(name)).unwrap();
    let transport = Scripted::new(vec![
        response(200, None, &read("refs.json")),
        response(200, None, &read("manifest.json")),
        response(200, None, &read("project.gfpb")),
    ]);
    match run_clone_with(&transport, clone_args(&destination), true, &mut Vec::new()) {
        Ok(()) => println!("CLONE_OK"),
        Err(error) => println!("CLONE_ERROR {error}"),
    }
}

/// A real bundle and a fixture directory the child helper clones from.
fn helper_fixture() -> (Vec<u8>, String, tempfile::TempDir) {
    let (bundle, package_digest) = real_bundle();
    let fixture = tempfile::tempdir().unwrap();
    let (refs, manifest) = clone_documents(&bundle, &package_digest, &[]);
    std::fs::write(fixture.path().join("refs.json"), &refs).unwrap();
    std::fs::write(fixture.path().join("manifest.json"), &manifest).unwrap();
    std::fs::write(fixture.path().join("project.gfpb"), &bundle).unwrap();
    (bundle, package_digest, fixture)
}

/// Clone `fixture` into `destination` in a child process with `failpoint` set.
fn clone_in_child(fixture: &Path, destination: &Path, failpoint: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", HELPER, "--nocapture", "--test-threads=1"])
        .env("GRAPHFORGE_CLONE_HELPER_FIXTURE", fixture)
        .env("GRAPHFORGE_CLONE_HELPER_DESTINATION", destination)
        .env("GRAPHFORGE_CLONE_FAILPOINT", failpoint)
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
        .output()
        .unwrap()
}

static RERUN_AVAILABLE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn a_rerun_after_the_import_committed_needs_space_only_for_the_remaining_work() {
    let (bundle, package_digest, fixture) = helper_fixture();
    let length = bundle.len() as u64;
    // A first run needs about 3.25x the package.
    assert!(required_space(length, length, 0, false) > length);
    for failpoint in ["portable_import.before_reopen", "clone.before_install"] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("openalex");
        let child = clone_in_child(fixture.path(), &destination, failpoint);
        assert_eq!(
            child.status.code(),
            Some(CLONE_FAILPOINT_EXIT),
            "{failpoint}"
        );
        // Bytes the killed run staged for the import are credited: offer
        // less than the package, so the rerun passes only with that credit.
        let staged = staged_import_bytes(&acquire_staging(&destination).unwrap());
        assert!(staged > 0, "{failpoint}: nothing staged");
        RERUN_AVAILABLE.store(length - staged / 2, Ordering::SeqCst);
        // Research validation scratch an interrupted run left behind is
        // removed before space is measured, never counted as credit.
        let scratch = staging_path(&destination)
            .unwrap()
            .join(format!("{RESEARCH_SCRATCH_PREFIX}left"));
        std::fs::create_dir(&scratch).unwrap();
        std::fs::write(scratch.join("bytes"), vec![0_u8; bundle.len() * 4]).unwrap();
        let mut env = CloneEnv {
            available_space: |root| {
                let stale = std::fs::read_dir(root)?.flatten().any(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(RESEARCH_SCRATCH_PREFIX)
                });
                if stale {
                    return Err(std::io::Error::other("stale scratch was not removed"));
                }
                Ok(RERUN_AVAILABLE.load(Ordering::SeqCst))
            },
            ..CloneEnv::quiet()
        };
        let mut output = Vec::new();
        run_clone_profiled_with_delays(
            &clone_script(&bundle, &package_digest),
            clone_args(&destination),
            true,
            &mut output,
            &TelemetryRuntime::default(),
            &CloneDelays::default(),
            &mut env,
        )
        .unwrap_or_else(|error| panic!("{failpoint}: rerun failed: {error}"));
        let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
        assert_reopens_at(&destination, &result);
    }
}

#[test]
fn a_destination_that_appears_during_the_clone_releases_the_staging() {
    let (bundle, package_digest) = real_bundle();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let appearing = destination.clone();
    let delays = CloneDelays {
        before_import: Some(Box::new(move || std::fs::create_dir(&appearing).unwrap())),
        ..CloneDelays::default()
    };
    let error = run_clone_profiled_with_delays(
        &clone_script(&bundle, &package_digest),
        clone_args(&destination),
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &delays,
        &mut CloneEnv::quiet(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("hub.destination_conflict"),
        "{error}"
    );
    assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
    assert!(
        names(&destination).is_empty(),
        "the other destination is untouched"
    );
}

#[test]
fn an_occupied_destination_releases_staging_a_killed_clone_left() {
    let (bundle, package_digest, fixture) = helper_fixture();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("openalex");
    let child = clone_in_child(fixture.path(), &destination, "clone.before_install");
    assert_eq!(child.status.code(), Some(CLONE_FAILPOINT_EXIT));
    std::fs::create_dir(&destination).unwrap();
    std::fs::write(destination.join("mine"), b"user data").unwrap();
    let error = run_clone_with(
        &clone_script(&bundle, &package_digest),
        clone_args(&destination),
        true,
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("hub.destination_conflict"),
        "{error}"
    );
    assert_eq!(names(root.path()), BTreeSet::from(["openalex".to_owned()]));
    assert_eq!(
        std::fs::read(destination.join("mine")).unwrap(),
        b"user data"
    );
}

#[test]
fn a_kill_during_cleanup_leaves_a_state_the_rerun_resolves() {
    let (bundle, package_digest, fixture) = helper_fixture();
    for keep_record in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("openalex");
        let child = clone_in_child(fixture.path(), &destination, "clone.after_install");
        assert_eq!(child.status.code(), Some(CLONE_FAILPOINT_EXIT));
        let staging = acquire_staging(&destination).unwrap();
        let entries = std::fs::read_dir(&staging.root)
            .unwrap()
            .flatten()
            .map(|entry| (entry.path(), entry.file_type().unwrap().is_dir()))
            .collect();
        // Remove what a cleanup killed part-way through would have removed:
        // everything before the record, and the record too when it went.
        let order = removal_order(&staging, entries);
        let stop = order.len() - if keep_record { 2 } else { 1 };
        for (path, directory) in &order[..stop] {
            if *directory {
                std::fs::remove_dir_all(path).unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
        }
        drop(staging);
        let mut output = Vec::new();
        let result = run_clone_with(
            &clone_script(&bundle, &package_digest),
            clone_args(&destination),
            true,
            &mut output,
        );
        if keep_record {
            result.unwrap();
            let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
            assert_reopens_at(&destination, &result);
        } else {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("hub.destination_conflict"),
                "{error}"
            );
        }
        let left = names(root.path());
        assert!(
            left.iter()
                .all(|name| name == "openalex" || name.starts_with(".graphforge-admission-")),
            "residue {left:?}"
        );
    }
}

#[test]
fn removal_order_puts_the_install_record_and_then_the_lock_last() {
    let root = tempfile::tempdir().unwrap();
    let staging = acquire_staging(&root.path().join("openalex")).unwrap();
    let entries = vec![
        (staging.lock.clone(), false),
        (staging.installed.clone(), false),
        (staging.project.clone(), true),
        (staging.partial.clone(), false),
    ];
    let order: Vec<_> = removal_order(&staging, entries)
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        &order[2..],
        [staging.installed.clone(), staging.lock.clone()]
    );
}

#[test]
fn an_unsatisfiable_range_discards_the_partial_and_restarts() {
    let bytes = payload();
    let descriptor = object(&bytes);
    let root = tempfile::tempdir().unwrap();
    let partial = root.path().join("package.part");
    std::fs::write(&partial, &bytes[..100]).unwrap();
    std::fs::write(
        partial.with_extension("resume.json"),
        serde_json::to_vec(&ResumeState {
            digest: descriptor.digest.0.clone(),
            length: bytes.len() as u64,
            location: descriptor.locations[0].clone(),
            etag: "\"fixture-1\"".into(),
        })
        .unwrap(),
    )
    .unwrap();
    let transport = Scripted::new(vec![response(416, None, b""), response(200, None, &bytes)]);
    assert_eq!(
        download(&transport, &descriptor, &partial).unwrap(),
        DownloadReport {
            resumed_bytes: 0,
            transferred_bytes: bytes.len() as u64,
            attempts: 2,
        }
    );
    assert_eq!(std::fs::read(&partial).unwrap(), bytes);
}

#[test]
fn killed_or_failed_clone_leaves_a_state_the_rerun_completes() {
    let (bundle, package_digest, fixture) = helper_fixture();
    for (failpoint, killed, installed) in [
        ("clone.after_download", true, false),
        ("portable_import.after_owner", true, false),
        ("project.after_writer_lock", true, false),
        ("project.after_manifest_fsync", true, false),
        ("project.after_current_replace", true, false),
        ("portable_import.before_reopen", true, false),
        ("clone.before_install", true, false),
        ("clone.after_install", true, true),
        ("project.after_writer_lock.error", false, false),
        ("portable_import.before_reopen.error", false, false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("openalex");
        let child = clone_in_child(fixture.path(), &destination, failpoint);
        let stdout = String::from_utf8_lossy(&child.stdout);
        if killed {
            assert_eq!(
                child.status.code(),
                Some(CLONE_FAILPOINT_EXIT),
                "{failpoint}: {stdout}"
            );
        } else {
            assert!(child.status.success(), "{failpoint}: {stdout}");
            // The import's own code, never a blanket integrity failure.
            assert!(
                stdout.contains("hub.package.io: portable project import failed")
                    && !stdout.contains("hub.integrity"),
                "{failpoint}: {stdout}"
            );
        }
        assert_eq!(
            destination.exists(),
            installed,
            "{failpoint}: the destination appears only complete"
        );
        let mut output = Vec::new();
        run_clone_with(
            &clone_script(&bundle, &package_digest),
            clone_args(&destination),
            true,
            &mut output,
        )
        .unwrap_or_else(|error| panic!("{failpoint}: rerun failed: {error}"));
        let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(result["package_digest"], package_digest, "{failpoint}");
        let left = names(root.path());
        assert!(
            left.iter().all(|name| name == "openalex"
                || (installed && name.starts_with(".graphforge-admission-"))),
            "{failpoint}: residue {left:?}"
        );
        assert_reopens_at(&destination, &result);
    }
}
