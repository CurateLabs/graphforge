//! Deterministic graph-workspace snapshots for project-generation publication.
//!
//! Execution operates on a private mutable workspace. A successful write
//! captures that workspace as one Arrow IPC participant, allowing `graphforge-api` to
//! publish graph and domain participants through one `CURRENT` transition.

use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_storage::{ProjectParticipant, ProjectParticipantEncoding};

const GRAPH_SNAPSHOT_CAPABILITY_VERSION: u32 = 1;
const GRAPH_SNAPSHOT_RECORD_VERSION: u32 = 1;
const MAX_SNAPSHOT_FILES: usize = 100_000;
const MAX_SNAPSHOT_FILE_BYTES: u64 = 1 << 30;
const MAX_SNAPSHOT_TOTAL_BYTES: u64 = 2 << 30;
const GRAPH_SNAPSHOT_SCHEMA_CANONICAL_BYTES: &[u8] =
    b"graph_snapshot/1|relative_path:utf8:not-null|content:binary:not-null";

static GRAPH_SNAPSHOT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("content", DataType::Binary, false),
    ]))
});

/// Capture every stable regular file below a private graph workspace.
///
/// # Errors
/// Rejects links, special files, invalid relative paths, and bounded-resource
/// violations before producing participant bytes.
pub(crate) fn capture(root: &Path) -> Result<ProjectParticipant, GfError> {
    let mut paths = Vec::new();
    collect_files(root, &mut paths)?;
    if paths.len() > MAX_SNAPSHOT_FILES {
        return Err(resource_limit("graph snapshot file count exceeds limit"));
    }

    let mut paths = paths
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| validation("graph snapshot path escaped workspace"))?;
            validate_relative_path(relative)?;
            Ok((path_text(relative)?, path))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    paths.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut relative_paths = Vec::with_capacity(paths.len());
    let mut contents = Vec::with_capacity(paths.len());
    let mut total = 0_u64;
    for (relative, path) in paths {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| storage("inspect graph snapshot file", &path, error))?;
        if !metadata.file_type().is_file() {
            return Err(validation("graph snapshot contains a non-regular file"));
        }
        if metadata.len() > MAX_SNAPSHOT_FILE_BYTES {
            return Err(resource_limit("graph snapshot file exceeds size limit"));
        }
        total = total
            .checked_add(metadata.len())
            .ok_or_else(|| resource_limit("graph snapshot total size overflow"))?;
        if total > MAX_SNAPSHOT_TOTAL_BYTES {
            return Err(resource_limit("graph snapshot total size exceeds limit"));
        }
        relative_paths.push(relative);
        contents.push(
            fs::read(&path).map_err(|error| storage("read graph snapshot file", &path, error))?,
        );
    }

    let batch = RecordBatch::try_new(
        Arc::clone(&GRAPH_SNAPSHOT_SCHEMA),
        vec![
            Arc::new(StringArray::from(relative_paths)) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(
                contents.iter().map(Vec::as_slice),
            )),
        ],
    )
    .map_err(|error| GfError::Execution(error.to_string()))?;
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &GRAPH_SNAPSHOT_SCHEMA)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| GfError::Execution(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| GfError::Execution(error.to_string()))?;
    }
    Ok(ProjectParticipant {
        capability_id: "graph".into(),
        capability_version: GRAPH_SNAPSHOT_CAPABILITY_VERSION,
        record_family_id: "snapshot".into(),
        record_version: GRAPH_SNAPSHOT_RECORD_VERSION,
        encoding: ProjectParticipantEncoding::Arrow,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            GRAPH_SNAPSHOT_SCHEMA_CANONICAL_BYTES,
        )
        .map_err(|error| GfError::Validation(error.to_string()))?,
        row_count: u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
        bytes,
    })
}

/// Hydrate a verified graph snapshot into an empty private workspace.
///
/// # Errors
/// Rejects schema drift, duplicate/unsorted/unsafe paths, malformed IPC, and
/// bounded-resource violations. No path can escape `target`.
pub(crate) fn hydrate(bytes: &[u8], target: &Path) -> Result<(), GfError> {
    if target
        .read_dir()
        .map_err(|error| storage("inspect graph workspace", target, error))?
        .next()
        .is_some()
    {
        return Err(validation("graph workspace must be empty before hydration"));
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| validation(format!("invalid graph snapshot IPC: {error}")))?;
    if reader.schema().as_ref() != GRAPH_SNAPSHOT_SCHEMA.as_ref() {
        return Err(validation("graph snapshot schema mismatch"));
    }

    let mut previous: Option<String> = None;
    let mut seen = HashSet::new();
    let mut count = 0_usize;
    let mut total = 0_u64;
    for batch in reader {
        let batch =
            batch.map_err(|error| validation(format!("invalid graph snapshot batch: {error}")))?;
        let paths = batch
            .column_by_name("relative_path")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| validation("graph snapshot path column mismatch"))?;
        let contents = batch
            .column_by_name("content")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| validation("graph snapshot content column mismatch"))?;
        for row in 0..batch.num_rows() {
            if paths.is_null(row) || contents.is_null(row) {
                return Err(validation("graph snapshot contains null values"));
            }
            count += 1;
            if count > MAX_SNAPSHOT_FILES {
                return Err(resource_limit("graph snapshot file count exceeds limit"));
            }
            let relative_text = paths.value(row);
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= relative_text)
            {
                return Err(validation(
                    "graph snapshot paths are duplicate or non-canonical",
                ));
            }
            if !seen.insert(relative_text.to_owned()) {
                return Err(validation("graph snapshot contains duplicate paths"));
            }
            let relative = Path::new(relative_text);
            validate_relative_path(relative)?;
            let content = contents.value(row);
            let length = u64::try_from(content.len()).unwrap_or(u64::MAX);
            if length > MAX_SNAPSHOT_FILE_BYTES {
                return Err(resource_limit("graph snapshot file exceeds size limit"));
            }
            total = total
                .checked_add(length)
                .ok_or_else(|| resource_limit("graph snapshot total size overflow"))?;
            if total > MAX_SNAPSHOT_TOTAL_BYTES {
                return Err(resource_limit("graph snapshot total size exceeds limit"));
            }
            let destination = target.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| storage("create graph snapshot directory", parent, error))?;
            }
            fs::write(&destination, content)
                .map_err(|error| storage("write graph snapshot file", &destination, error))?;
            previous = Some(relative_text.to_owned());
        }
    }
    Ok(())
}

/// Replace private workspace contents with one previously captured snapshot.
pub(crate) fn restore(bytes: &[u8], target: &Path) -> Result<(), GfError> {
    for entry in target
        .read_dir()
        .map_err(|error| storage("read graph workspace for restore", target, error))?
    {
        let entry = entry.map_err(|error| storage("read graph workspace entry", target, error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect graph workspace entry", &path, error))?;
        if file_type.is_dir() {
            fs::remove_dir_all(&path)
                .map_err(|error| storage("remove graph workspace directory", &path, error))?;
        } else {
            fs::remove_file(&path)
                .map_err(|error| storage("remove graph workspace file", &path, error))?;
        }
    }
    hydrate(bytes, target)
}

fn collect_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), GfError> {
    let mut entries = directory
        .read_dir()
        .map_err(|error| storage("read graph workspace", directory, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| storage("read graph workspace entry", directory, error))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| storage("inspect graph workspace entry", &path, error))?;
        if file_type.is_symlink() {
            return Err(validation("graph workspace contains a symbolic link"));
        }
        if file_type.is_dir() {
            collect_files(&path, paths)?;
        } else if file_type.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".lock") || name.starts_with(".gf-stage-") {
                continue;
            }
            paths.push(path);
        } else {
            return Err(validation("graph workspace contains a special file"));
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), GfError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || matches!(component, Component::ParentDir | Component::RootDir)
        })
    {
        return Err(validation("invalid graph snapshot relative path"));
    }
    let _ = path_text(path)?;
    Ok(())
}

fn path_text(path: &Path) -> Result<String, GfError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| validation("graph snapshot path is not UTF-8"))
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Execution(format!("GF_RESOURCE_LIMIT: {}", message.into()))
}

fn storage(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("{action} at {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trip_is_byte_deterministic_and_path_ordered() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("topology/edges")).unwrap();
        fs::create_dir_all(source.path().join("derived/uuid-membership")).unwrap();
        fs::write(source.path().join("topology/nodes.parquet"), b"nodes").unwrap();
        fs::write(source.path().join("topology/edges/knows.parquet"), b"edges").unwrap();
        fs::write(
            source.path().join("derived/uuid-membership.json"),
            b"manifest",
        )
        .unwrap();
        fs::write(
            source.path().join("derived/uuid-membership/run.uuidx"),
            b"run",
        )
        .unwrap();
        fs::write(source.path().join("writer.lock"), b"ignored").unwrap();

        let first = capture(source.path()).unwrap();
        let second = capture(source.path()).unwrap();
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.row_count, 4);

        let target = tempfile::tempdir().unwrap();
        hydrate(&first.bytes, target.path()).unwrap();
        assert_eq!(
            fs::read(target.path().join("topology/nodes.parquet")).unwrap(),
            b"nodes"
        );
        assert_eq!(
            fs::read(target.path().join("topology/edges/knows.parquet")).unwrap(),
            b"edges"
        );
        assert!(!target.path().join("writer.lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn capture_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let source = tempfile::tempdir().unwrap();
        symlink("/tmp", source.path().join("escape")).unwrap();
        assert!(capture(source.path()).is_err());
    }
}
