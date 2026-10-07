//! Parallel artifact installation: every published byte is written once, and
//! SHA-256 and XXH64 are computed from the bytes about to be written.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Mutex;

use parquet::arrow::ArrowWriter;

use super::*;

pub(super) struct Installer<'a> {
    output: &'a StableDirectory,
    directories: Mutex<HashMap<String, StableDirectory>>,
    artifacts: Mutex<Vec<ConstructionEncodedArtifact>>,
    written: std::sync::atomic::AtomicU64,
}

impl<'a> Installer<'a> {
    pub(super) fn new(output: &'a StableDirectory) -> Self {
        Self {
            output,
            directories: Mutex::new(HashMap::new()),
            artifacts: Mutex::new(Vec::new()),
            written: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn directory(&self, parent: &str) -> Result<StableDirectory, GfError> {
        let mut directories = self
            .directories
            .lock()
            .map_err(|_| storage("bulk directory cache poisoned"))?;
        if let Some(directory) = directories.get(parent) {
            return directory.try_clone().map_err(storage);
        }
        let mut directory = self
            .output
            .create_child_directory(OsStr::new("graph"))
            .map_err(storage)?;
        for component in Path::new(parent).components() {
            let Component::Normal(name) = component else {
                return Err(storage("canonical artifact path is not normalized"));
            };
            directory = directory.create_child_directory(name).map_err(storage)?;
        }
        directories.insert(parent.to_owned(), directory.try_clone().map_err(storage)?);
        Ok(directory)
    }

    /// Write `bytes` to `relative` below the graph root and record its digests.
    pub(super) fn install(&self, relative: &str, bytes: &[u8]) -> Result<(), GfError> {
        let (parent, name) = relative
            .rsplit_once('/')
            .map_or(("", relative), |(parent, name)| (parent, name));
        let directory = self.directory(parent)?;
        let mut digest = Sha256::new();
        digest.update(bytes);
        let mut checksum = crate::corruption_checksum::Checksum::new();
        checksum.update(bytes);
        let mut file = directory
            .create_replaceable_child_file(OsStr::new(name))
            .map_err(storage)?;
        file.write_all(bytes).map_err(storage)?;
        directory
            .observe_file(OsStr::new(name), &file)
            .map_err(storage)?;
        self.written
            .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.artifacts
            .lock()
            .map_err(|_| storage("bulk artifact list poisoned"))?
            .push(ConstructionEncodedArtifact {
                path: relative.to_owned(),
                bytes: bytes.len() as u64,
                sha256: hex(&digest.finalize()),
                xxh64: checksum.finish(),
            });
        Ok(())
    }

    /// Encode `batch` as one permanent-policy Parquet object and install it.
    pub(super) fn install_parquet(
        &self,
        relative: &str,
        batch: &RecordBatch,
    ) -> Result<(), GfError> {
        let mut writer = ArrowWriter::try_new(
            Vec::with_capacity(batch.get_array_memory_size() / 2),
            batch.schema(),
            Some(crate::permanent_parquet::writer_properties().build()),
        )
        .map_err(storage)?;
        writer.write(batch).map_err(storage)?;
        let bytes = writer.into_inner().map_err(storage)?;
        self.install(relative, &bytes)
    }

    pub(super) fn written_bytes(&self) -> u64 {
        self.written.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) fn into_artifacts(self) -> Result<Vec<ConstructionEncodedArtifact>, GfError> {
        self.artifacts
            .into_inner()
            .map_err(|_| storage("bulk artifact list poisoned"))
    }

    /// Register artifacts a delegate wrote itself.
    pub(super) fn extend(
        &self,
        artifacts: impl IntoIterator<Item = ConstructionEncodedArtifact>,
    ) -> Result<(), GfError> {
        self.artifacts
            .lock()
            .map_err(|_| storage("bulk artifact list poisoned"))?
            .extend(artifacts);
        Ok(())
    }
}
