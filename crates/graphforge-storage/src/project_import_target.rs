//! Shared no-follow file and pristine target admission for native project publication.
use crate::{ProjectParticipantEncoding, ResolvedProjectGeneration, resolve_project_generation};
use graphforge_core::{GfError, ProjectErrorCode};
use std::path::Path;
use uuid::Uuid;
pub(crate) fn prepare_import_target(
    target: &Path,
) -> Result<Option<ResolvedProjectGeneration>, GfError> {
    reject_symlink_components(target, "portable import target")?;
    match std::fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(project_error(
                ProjectErrorCode::UnsupportedProjectFormat,
                "portable import target is linked or not a directory",
            ))
        }
        Ok(_) => {
            let is_empty = std::fs::read_dir(target)
                .map_err(|error| {
                    GfError::Storage(format!("failed to inspect portable import target: {error}"))
                })?
                .next()
                .is_none();
            if is_empty {
                Ok(None)
            } else if is_pristine_initialized_target(target)? {
                resolve_project_generation(target).map(Some)
            } else {
                Err(project_error(
                    ProjectErrorCode::UnsupportedProjectFormat,
                    "portable import target must be empty or pristine",
                ))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(project_error(
            ProjectErrorCode::UnsupportedFilesystem,
            "portable import target was not admitted before target preparation",
        )),
        Err(error) => Err(GfError::Storage(format!(
            "failed to inspect portable import target: {error}"
        ))),
    }
}

fn is_pristine_initialized_target(target: &Path) -> Result<bool, GfError> {
    let Some(generation_uuid) = semantically_pristine_generation(target)? else {
        return Ok(false);
    };
    has_exact_pristine_layout(target, generation_uuid)
}

pub(crate) fn semantically_pristine_generation(target: &Path) -> Result<Option<Uuid>, GfError> {
    let Ok(resolved) = resolve_project_generation(target) else {
        return Ok(None);
    };
    let capabilities = resolved.capabilities();
    if capabilities.len() != 2
        || capabilities[0].capability_id != "graph"
        || capabilities[0].capability_version != 1
        || capabilities[1].capability_id != "workspace"
        || capabilities[1].capability_version != 1
    {
        return Ok(None);
    }
    let actual = resolved.participant_snapshots()?;
    let expected = crate::workspace_participants::empty_workspace_participants()?;
    if actual.len() != expected.len()
        || !actual.iter().zip(&expected).all(|(actual, expected)| {
            actual.capability_id == expected.capability_id
                && actual.capability_version == expected.capability_version
                && actual.record_family_id == expected.record_family_id
                && actual.record_version == expected.record_version
                && actual.encoding
                    == match expected.encoding {
                        ProjectParticipantEncoding::Parquet => "parquet",
                        ProjectParticipantEncoding::Arrow => "arrow",
                        ProjectParticipantEncoding::Json => "json",
                    }
                && actual.schema_fingerprint == expected.schema_fingerprint
                && actual.row_count == expected.row_count
                && actual.bytes == expected.bytes
        })
    {
        return Ok(None);
    }
    Ok(Some(resolved.generation_uuid()))
}

fn has_exact_pristine_layout(target: &Path, generation_uuid: Uuid) -> Result<bool, GfError> {
    let root_names = directory_names(target)?;
    if root_names != ["CURRENT", "FORMAT", "generations"] {
        return Ok(false);
    }
    let generations = target.join("generations");
    if directory_names(&generations)? != [generation_uuid.hyphenated().to_string()] {
        return Ok(false);
    }
    let generation = generations.join(generation_uuid.hyphenated().to_string());
    if directory_names(&generation)? != ["lease.lock", "manifest.json", "participants"] {
        return Ok(false);
    }
    let participants = generation.join("participants");
    if directory_names(&participants)? != ["workspace"] {
        return Ok(false);
    }
    Ok(directory_names(&participants.join("workspace"))?
        == [
            "configuration.json",
            "ontology.json",
            "research_metadata.json",
        ])
}

fn directory_names(path: &Path) -> Result<Vec<String>, GfError> {
    let mut names = std::fs::read_dir(path)
        .map_err(|error| {
            GfError::Storage(format!("failed to inspect portable import target: {error}"))
        })?
        .map(|entry| {
            entry
                .map_err(|error| {
                    GfError::Storage(format!("failed to inspect portable import target: {error}"))
                })?
                .file_name()
                .into_string()
                .map_err(|_| {
                    project_error(
                        ProjectErrorCode::UnsupportedProjectFormat,
                        "portable import target contains a non-UTF-8 entry",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

fn reject_symlink_components(path: &Path, name: &str) -> Result<(), GfError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                GfError::Storage(format!("failed to resolve current directory: {error}"))
            })?
            .join(path)
    };
    for component in absolute.ancestors() {
        match std::fs::symlink_metadata(component) {
            Ok(metadata)
                if metadata.file_type().is_symlink() && !trusted_platform_symlink(&metadata) =>
            {
                return Err(project_error(
                    ProjectErrorCode::UnsupportedFilesystem,
                    format!("{name} has a symbolic-link path component"),
                ));
            }
            Ok(metadata)
                if component != absolute
                    && !metadata.is_dir()
                    && !metadata.file_type().is_symlink() =>
            {
                return Err(project_error(
                    ProjectErrorCode::UnsupportedFilesystem,
                    format!("{name} has a non-directory ancestor"),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(GfError::Storage(format!(
                    "failed to inspect {name} path components: {error}"
                )));
            }
        }
    }
    Ok(())
}

// macOS exposes stable system prefixes such as `/var` through root-owned
// links. Those are outside a caller's control; project-local links are not.
#[cfg(unix)]
fn trusted_platform_symlink(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.uid() == 0
}

#[cfg(not(unix))]
fn trusted_platform_symlink(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
pub(crate) fn open_regular_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
pub(crate) fn open_regular_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn linked_target_ancestor_is_rejected_before_admission() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let target = alias.join("destination");
        assert_eq!(
            prepare_import_target(&target).unwrap_err().code(),
            "GF_UNSUPPORTED_FILESYSTEM"
        );
        assert!(!real.join("destination").exists());
        assert_eq!(std::fs::read_dir(real).unwrap().count(), 0);
    }
}
