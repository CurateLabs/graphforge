//! Peak live heap of OCI publish and pull of a bundle far larger than the copy
//! buffer (#1872), through the public registry-injection entry points.
//!
//! A counting global allocator measures every live byte the process holds while
//! one publish (or pull) runs. The registry keeps layers on disk, so the only
//! way the peak can scale with the bundle is the library buffering the layer.
#![allow(
    unsafe_code,
    reason = "a counting global allocator wraps System and only observes sizes"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use graphforge_api::{
    GraphForge, IrLiteral, MemoryOciRegistry, PortableSelection, PortableV2ExportRequest,
    PortableV2OciPublishRequest, PortableV2OciPullRequest, PortableV2OciRegistry,
    publish_portable_v2_oci_with_registry, pull_portable_v2_oci_with_registry,
};
use graphforge_core::portable::{PortableV2Error, PortableV2ErrorCode, PortableV2Limits};
use graphforge_portable_oci::{DigestingReader, MEMORY_REGISTRY_MAX_BLOB_BYTES, digest_reader};
use graphforge_storage::{PortableV2Output, PortableV2SelectionProfile};

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let now = CURRENT.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every method defers to `System`, which upholds the `GlobalAlloc`
// contract; the counters only observe sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            // A reallocation may copy, so the old and the new block are both
            // live at once; count it that way instead of assuming it is in place.
            grow(new);
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The counters are process-wide, so the tests here do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Run `operation`; return its result and the most bytes it held live at once
/// above where it started.
fn peak_live_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let result = operation();
    (
        result,
        PEAK.load(Ordering::Relaxed).saturating_sub(baseline),
    )
}

/// Keeps layers in files, so the registry never holds one in memory.
struct DiskLayerRegistry {
    dir: PathBuf,
    small: MemoryOciRegistry,
}

impl DiskLayerRegistry {
    fn layer(&self, digest: &str) -> PathBuf {
        self.dir.join(digest.replace(':', "-"))
    }
}

impl PortableV2OciRegistry for DiskLayerRegistry {
    fn put_blob(&self, r: &str, d: &str, b: &[u8]) -> Result<(), PortableV2Error> {
        self.small.put_blob(r, d, b)
    }

    fn put_blob_stream(
        &self,
        _repository: &str,
        digest: &str,
        size: u64,
        source: &mut dyn Read,
    ) -> Result<(), PortableV2Error> {
        let io = |_| PortableV2Error::new(PortableV2ErrorCode::Io, "layer file failed");
        let path = self.layer(digest);
        let mut file = fs::File::create(&path).map_err(io)?;
        let mut source = DigestingReader::new(source);
        std::io::copy(&mut source, &mut file).map_err(io)?;
        if source.finish() != (digest.to_owned(), size) {
            fs::remove_file(&path).map_err(io)?;
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::DigestMismatch,
                "layer does not match its digest or size",
            ));
        }
        Ok(())
    }

    fn get_blob_stream<'a>(
        &'a self,
        _repository: &str,
        digest: &str,
        _size: u64,
    ) -> Result<Box<dyn Read + 'a>, PortableV2Error> {
        fs::File::open(self.layer(digest))
            .map(|file| Box::new(file) as Box<dyn Read>)
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "no layer"))
    }

    fn get_blob(&self, r: &str, d: &str) -> Result<Vec<u8>, PortableV2Error> {
        self.small.get_blob(r, d)
    }

    fn blob_exists(&self, r: &str, d: &str) -> Result<bool, PortableV2Error> {
        Ok(self.layer(d).exists() || self.small.blob_exists(r, d)?)
    }

    fn put_manifest(
        &self,
        r: &str,
        reference: &str,
        media: &str,
        b: &[u8],
    ) -> Result<String, PortableV2Error> {
        self.small.put_manifest(r, reference, media, b)
    }

    fn get_manifest(&self, r: &str, reference: &str) -> Result<(String, Vec<u8>), PortableV2Error> {
        self.small.get_manifest(r, reference)
    }
}

/// A verified bundle dominated by `docs` pseudo-random string properties.
fn export_large_bundle(dir: &Path, docs: usize, doc_bytes: usize) -> PathBuf {
    let graph = GraphForge::new(dir.join("source").to_str()).unwrap();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..docs {
        let body: String = (0..doc_bytes)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(b'a' + (state % 26) as u8)
            })
            .collect();
        graph
            .execute_with_params(
                "CREATE (:Doc {body:$body})",
                &HashMap::from([("body".to_owned(), IrLiteral::Str(body.into()))]),
            )
            .unwrap();
    }
    let package = dir.join("large.gfpb");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    package
}

#[test]
fn publish_and_pull_hold_a_fixed_buffer_regardless_of_bundle_size() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let bundle = export_large_bundle(dir.path(), DOCS, 1024 * 1024);
    let bundle_bytes = fs::metadata(&bundle).unwrap().len() as usize;
    let limits = PortableV2Limits {
        copy_buffer_bytes: COPY_BUFFER_BYTES,
        ..PortableV2Limits::default()
    };
    let layers = dir.path().join("layers");
    fs::create_dir(&layers).unwrap();
    let registry = DiskLayerRegistry {
        dir: layers,
        small: MemoryOciRegistry::default(),
    };

    let (published, publish_peak) = peak_live_bytes(|| {
        publish_portable_v2_oci_with_registry(
            &registry,
            &PortableV2OciPublishRequest {
                package_path: &bundle,
                registry: "disk.local",
                repository: "tests/large",
                tag: None,
                limits,
                authenticity: Default::default(),
                signature: None,
                credential: None,
            },
            None,
        )
        .unwrap()
    });
    let destination = dir.path().join("pulled.gfpb");
    let (pulled, pull_peak) = peak_live_bytes(|| {
        pull_portable_v2_oci_with_registry(
            &registry,
            &PortableV2OciPullRequest {
                registry: "disk.local",
                repository: "tests/large",
                reference: &published.oci_manifest_digest,
                expected_oci_digest: None,
                destination: &destination,
                limits,
                authenticity: Default::default(),
                credential: None,
            },
            None,
        )
        .unwrap()
    });
    eprintln!("bundle={bundle_bytes} publish_peak={publish_peak} pull_peak={pull_peak}");
    assert_eq!(pulled.report.package_digest, published.package_digest);
    assert_eq!(fs::read(&destination).unwrap(), fs::read(&bundle).unwrap());
    // Each operation holds a few copy buffers, and the bundle is many times that.
    assert!(
        publish_peak < PEAK_BOUND_BYTES,
        "publish held {publish_peak} bytes"
    );
    assert!(pull_peak < PEAK_BOUND_BYTES, "pull held {pull_peak} bytes");
    assert!(bundle_bytes > 16 * PEAK_BOUND_BYTES);
}

const DOCS: usize = 32;
const COPY_BUFFER_BYTES: usize = 256 * 1024;
const PEAK_BOUND_BYTES: usize = 1024 * 1024;

/// Yields `remaining` deterministic bytes without holding them.
struct Generated {
    remaining: usize,
    next: u8,
}

impl Read for Generated {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = buf.len().min(self.remaining);
        for byte in &mut buf[..read] {
            *byte = self.next;
            self.next = self.next.wrapping_mul(31).wrapping_add(7);
        }
        self.remaining -= read;
        Ok(read)
    }
}

/// The memory registry keeps what it is given, so its retained bytes are the
/// layer. Uploading must not add a second whole-layer copy on top of that.
#[test]
fn memory_registry_upload_adds_no_second_copy_of_the_layer() {
    let _serial = serial();
    const LAYER: usize = 8 * 1024 * 1024;
    const OVERHEAD_BOUND: usize = 512 * 1024;
    let generated = || Generated {
        remaining: LAYER,
        next: 1,
    };
    let (digest, size) = digest_reader(&mut generated(), 64 * 1024).unwrap();
    let registry = MemoryOciRegistry::default();
    let baseline = CURRENT.load(Ordering::Relaxed);
    let (uploaded, peak) = peak_live_bytes(|| {
        registry.put_blob_stream("tests/large", &digest, size, &mut generated())
    });
    uploaded.unwrap();
    let retained = CURRENT.load(Ordering::Relaxed).saturating_sub(baseline);
    eprintln!("layer={LAYER} retained={retained} peak={peak}");
    assert!(retained >= LAYER, "the registry holds the layer");
    assert!(
        peak.saturating_sub(retained) < OVERHEAD_BOUND,
        "upload held {} bytes beyond the {retained} it retained",
        peak.saturating_sub(retained)
    );
}

/// A declared size is a claim, not a measurement: over the registry's limit it
/// is refused before anything is reserved for it.
#[test]
fn memory_registry_refuses_an_oversized_declared_size_before_reserving() {
    let _serial = serial();
    let registry = MemoryOciRegistry::default();
    let digest = graphforge_portable_oci::digest_sha256(b"");
    let (outcome, peak) = peak_live_bytes(|| {
        registry.put_blob_stream(
            "tests/large",
            &digest,
            MEMORY_REGISTRY_MAX_BLOB_BYTES + 1,
            &mut std::io::empty(),
        )
    });
    assert_eq!(
        outcome.unwrap_err().code,
        PortableV2ErrorCode::LimitExceeded
    );
    assert!(peak < 64 * 1024, "refusal still reserved {peak} bytes");
}
