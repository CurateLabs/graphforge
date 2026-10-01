//! Verified portable component materialization and cleanup.

use super::{
    ComponentSink, PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode,
    PortableV2Report, VerifiedMaterialization, verify_portable_v2_into,
};
use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Fully verify a package, then stream its authenticated component entries
/// into a new private directory for an importer. The destination is removed on
/// every error and is never a project publication boundary.
pub fn materialize_verified_portable_v2(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2Report, PortableV2Error> {
    materialize_verified_portable_v2_observed(
        source,
        destination,
        limits,
        cancelled,
        |_, _| Ok(()),
        false,
    )
    .map(|materialized| materialized.report)
}

pub(crate) fn materialize_verified_portable_v2_observed(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    mut observed: impl FnMut(&Path, Option<&File>) -> Result<(), PortableV2Error>,
    track_removals: bool,
) -> Result<VerifiedMaterialization, PortableV2Error> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    if destination.exists() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "materialization destination exists",
        ));
    }
    fs::create_dir(destination).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot create materialization")
    })?;
    let mut routes = std::collections::BTreeSet::new();
    let result = (|| {
        let mut tracking = |path: &Path, file: Option<&File>| {
            if track_removals {
                routes.insert(path.to_path_buf());
            }
            observed(path, file)
        };
        // One pass authenticates every member and writes each component to
        // the private destination as it is hashed, so the destination holds
        // exactly the authenticated bytes and no second verification is needed
        // to detect a source changing between verify and copy.
        let mut sink = ComponentSink {
            destination,
            observed: &mut tracking,
            written_bytes: 0,
            write_operations: 0,
        };
        let report = verify_portable_v2_into(
            source,
            PortableV2Mode::Full,
            limits,
            cancelled,
            Some(&mut sink),
        )?;
        let (application_read_bytes, application_read_operations) =
            (sink.written_bytes, sink.write_operations);
        sync_materialized_tree(destination)?;
        Ok(VerifiedMaterialization {
            report,
            application_read_bytes,
            application_read_operations,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(destination);
        for path in routes {
            if matches!(fs::symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                let _ = observed(&path, None);
            }
        }
    }
    result
}

fn sync_materialized_tree(root: &Path) -> Result<(), PortableV2Error> {
    let mut directories = vec![root.to_owned()];
    let mut index = 0;
    while index < directories.len() {
        for entry in fs::read_dir(&directories[index]).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read staged directory")
        })? {
            let entry = entry.map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read staged entry")
            })?;
            if entry
                .file_type()
                .map_err(|_| {
                    PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inspect staged entry")
                })?
                .is_dir()
            {
                directories.push(entry.path());
            }
        }
        index += 1;
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        crate::project_publication::sync_directory(&directory).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot sync staged directory")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
