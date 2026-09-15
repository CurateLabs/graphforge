//! Verified portable component materialization and cleanup.

use super::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2Report,
    VerifiedMaterialization, check_cancel, header_path, parse_octal, parse_pax,
    read_unhashed_payload, skip_padding, validate_path, verify_portable_v2, walk,
};
use std::fs::{self, File};
use std::io::{Read, Write};
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
    let before = fs::metadata(source)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "source unavailable"))?;
    let report = verify_portable_v2(source, PortableV2Mode::Full, limits, cancelled)?;
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
        let result = if before.is_dir() {
            materialize_expanded(source, destination, limits, cancelled, &mut tracking)
        } else {
            materialize_bundle(source, destination, limits, cancelled, &mut tracking)
        };
        let (application_read_bytes, application_read_operations) = result?;
        let after = fs::metadata(source).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "source disappeared",
            )
        })?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "source changed after verification",
            ));
        }
        let after_report = verify_portable_v2(source, PortableV2Mode::Full, limits, cancelled)
            .map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::ConcurrentMutation,
                    "source changed during materialization",
                )
            });
        let after_report = after_report?;
        if report.package_digest != after_report.package_digest
            || report.transport_digest != after_report.transport_digest
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "source changed during materialization",
            ));
        }
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

pub(super) fn materialize_expanded(
    source: &Path,
    destination: &Path,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    observed: &mut impl FnMut(&Path, Option<&File>) -> Result<(), PortableV2Error>,
) -> Result<(u64, u64), PortableV2Error> {
    let mut read_bytes = 0_u64;
    let mut read_operations = 0_u64;
    let mut paths = Vec::new();
    walk(source, source, &mut paths, limits, cancelled)?;
    for relative in paths
        .into_iter()
        .filter(|path| path.starts_with("data/components/"))
    {
        check_cancel(cancelled)?;
        let input_path = source.join(&relative);
        let before = fs::metadata(&input_path).map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, &relative, "cannot stat entry")
        })?;
        let output_path = destination.join(&relative);
        create_materialized_parent(&output_path, &relative)?;
        let mut input = File::open(&input_path).map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, &relative, "cannot open entry")
        })?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output_path)
            .map_err(|_| {
                PortableV2Error::at(PortableV2ErrorCode::Io, &relative, "cannot stage entry")
            })?;
        observed(&output_path, Some(&output))?;
        let copied =
            copy_materialized(&mut input, &mut output, limits.copy_buffer_bytes, cancelled);
        let refreshed = observed(&output_path, Some(&output));
        let (bytes, operations) = copied?;
        refreshed?;
        read_bytes = read_bytes.saturating_add(bytes);
        read_operations = read_operations.saturating_add(operations);
        let synced = output.sync_all().map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, &relative, "cannot sync entry")
        });
        let refreshed = observed(&output_path, Some(&output));
        synced?;
        refreshed?;
        let after = fs::metadata(&input_path).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                &relative,
                "entry disappeared",
            )
        })?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                &relative,
                "entry changed during materialization",
            ));
        }
    }
    sync_materialized_tree(destination)?;
    Ok((read_bytes, read_operations))
}

pub(super) fn materialize_bundle(
    source: &Path,
    destination: &Path,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    observed: &mut impl FnMut(&Path, Option<&File>) -> Result<(), PortableV2Error>,
) -> Result<(u64, u64), PortableV2Error> {
    let mut read_bytes = 0_u64;
    let mut read_operations = 0_u64;
    let mut input = File::open(source)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot reopen bundle"))?;
    let mut pending_pax = None;
    loop {
        check_cancel(cancelled)?;
        let mut header = [0u8; 512];
        input.read_exact(&mut header).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "truncated bundle")
        })?;
        if header.iter().all(|byte| *byte == 0) {
            let mut second = [0u8; 512];
            input.read_exact(&mut second).map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "truncated end marker",
                )
            })?;
            break;
        }
        let size = parse_octal(&header[124..136])?;
        let raw_path = header_path(&header)?;
        if header[156] == b'x' {
            let bytes = read_unhashed_payload(&mut input, size, limits.max_path_bytes + 32)?;
            pending_pax = Some(parse_pax(std::str::from_utf8(&bytes).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "PAX path is not UTF-8")
            })?)?);
            continue;
        }
        let path = pending_pax.take().unwrap_or(raw_path);
        validate_path(&path, limits.max_path_bytes)?;
        if path.starts_with("data/components/") {
            let output_path = destination.join(&path);
            create_materialized_parent(&output_path, &path)?;
            let mut output = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output_path)
                .map_err(|_| {
                    PortableV2Error::at(PortableV2ErrorCode::Io, &path, "cannot stage entry")
                })?;
            observed(&output_path, Some(&output))?;
            let copied = copy_exact_materialized(
                &mut input,
                &mut output,
                size,
                limits.copy_buffer_bytes,
                cancelled,
            );
            let refreshed = observed(&output_path, Some(&output));
            let (bytes, operations) = copied?;
            refreshed?;
            read_bytes = read_bytes.saturating_add(bytes);
            read_operations = read_operations.saturating_add(operations);
            let synced = output.sync_all().map_err(|_| {
                PortableV2Error::at(PortableV2ErrorCode::Io, &path, "cannot sync entry")
            });
            let refreshed = observed(&output_path, Some(&output));
            synced?;
            refreshed?;
        } else {
            skip_exact(&mut input, size, limits.copy_buffer_bytes, cancelled)?;
        }
        skip_padding(&mut input, size)?;
    }
    sync_materialized_tree(destination)?;
    Ok((read_bytes, read_operations))
}

fn create_materialized_parent(path: &Path, entry: &str) -> Result<(), PortableV2Error> {
    fs::create_dir_all(path.parent().expect("component entry has a parent")).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::Io,
            entry,
            "cannot create staged parent",
        )
    })
}
fn copy_materialized(
    input: &mut File,
    output: &mut impl Write,
    buffer_size: usize,
    cancelled: Option<&AtomicBool>,
) -> Result<(u64, u64), PortableV2Error> {
    let mut buffer = vec![0; buffer_size];
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    loop {
        check_cancel(cancelled)?;
        let count = input
            .read(&mut buffer)
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read entry"))?;
        if count == 0 {
            return Ok((bytes, operations));
        }
        bytes = bytes.saturating_add(count as u64);
        operations = operations.saturating_add(1);
        output
            .write_all(&buffer[..count])
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot stage entry"))?;
    }
}
fn copy_exact_materialized(
    input: &mut File,
    output: &mut impl Write,
    length: u64,
    buffer_size: usize,
    cancelled: Option<&AtomicBool>,
) -> Result<(u64, u64), PortableV2Error> {
    let mut remaining = length;
    let mut buffer = vec![0; buffer_size];
    while remaining > 0 {
        check_cancel(cancelled)?;
        let count = usize::try_from(remaining.min(buffer_size as u64)).unwrap();
        input.read_exact(&mut buffer[..count]).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "truncated payload")
        })?;
        output
            .write_all(&buffer[..count])
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot stage entry"))?;
        remaining -= count as u64;
    }
    let operations = length.div_ceil(buffer_size as u64);
    Ok((length, operations))
}
fn skip_exact(
    input: &mut File,
    length: u64,
    buffer_size: usize,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableV2Error> {
    let mut sink = std::io::sink();
    copy_exact_materialized(input, &mut sink, length, buffer_size, cancelled).map(|_| ())
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
