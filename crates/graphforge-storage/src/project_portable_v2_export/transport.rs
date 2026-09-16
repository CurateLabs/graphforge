//! Authenticated expanded and canonical bundle transport writing.
use super::{
    BAG_INFO, BAGIT, ExportAllocationObserver, ExportError, File, Identity, Path, PathBuf,
    PlannedFile, PlannedSource, PortableV2ExportLimits, PortableV2ExportPlan,
    PortableV2ExportProgress, err, fs, hex, identity, limit, observed_write_result,
    open_source_no_follow, storage, sync_dir,
};
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

pub(super) fn expanded(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    l: PortableV2ExportLimits,
    cancelled: &impl Fn() -> bool,
    progress: &mut impl FnMut(PortableV2ExportProgress),
    allocation: &mut ExportAllocationObserver,
) -> Result<[u8; 32], ExportError> {
    fs::create_dir(stage).map_err(storage)?;
    write_bytes(
        stage,
        "data/graphforge-project.json",
        &plan.manifest,
        allocation,
    )?;
    let mut payload = vec![(
        "data/graphforge-project.json".into(),
        plan.manifest.len() as u64,
        Sha256::digest(&plan.manifest).into(),
    )];
    let mut done = 0;
    for (i, f) in plan.files.iter().enumerate() {
        let target = stage.join(&f.path);
        parent(&target)?;
        copy(
            f,
            &target,
            l.copy_buffer_bytes,
            cancelled,
            allocation,
            |n| {
                done += n;
                progress(PortableV2ExportProgress {
                    entries_completed: i + 1,
                    bytes_completed: done,
                    entries_total: plan.files.len() + 5,
                    bytes_total: plan.payload_bytes,
                });
            },
        )?;
        progress(PortableV2ExportProgress {
            entries_completed: i + 2,
            bytes_completed: done,
            entries_total: plan.files.len() + 5,
            bytes_total: plan.payload_bytes,
        });
        payload.push((f.path.clone(), f.length, f.digest));
    }
    payload.sort_by(|a, b| a.0.cmp(&b.0));
    let inv = inventory(&payload, l.max_tag_manifest_bytes)?;
    write_bytes(stage, "manifest-sha256.txt", &inv, allocation)?;
    write_bytes(stage, "bagit.txt", BAGIT, allocation)?;
    write_bytes(stage, "bag-info.txt", BAG_INFO, allocation)?;
    let tags = [
        ("bag-info.txt", BAG_INFO),
        ("bagit.txt", BAGIT),
        ("manifest-sha256.txt", inv.as_slice()),
    ];
    let tag_rows = tags
        .iter()
        .map(|(p, b)| (p.to_string(), b.len() as u64, Sha256::digest(b).into()))
        .collect::<Vec<_>>();
    let tag = inventory(&tag_rows, l.max_tag_manifest_bytes)?;
    write_bytes(stage, "tagmanifest-sha256.txt", &tag, allocation)?;
    progress(PortableV2ExportProgress {
        entries_completed: plan.files.len() + 5,
        bytes_completed: done,
        entries_total: plan.files.len() + 5,
        bytes_total: plan.payload_bytes,
    });
    sync_tree(stage)?;
    let mut all = payload;
    all.extend(tag_rows);
    all.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = Sha256::new();
    h.update(b"graphforge-expanded/2\0");
    for (p, n, d) in all {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p);
        h.update(n.to_be_bytes());
        h.update(d);
    }
    h.update(tag);
    Ok(h.finalize().into())
}

pub(super) enum Src<'a> {
    Bytes(Vec<u8>),
    File(&'a PlannedFile),
}
impl Src<'_> {
    pub(super) fn len(&self) -> u64 {
        match self {
            Self::Bytes(b) => b.len() as u64,
            Self::File(f) => f.length,
        }
    }
}
pub(super) fn entries(
    plan: &PortableV2ExportPlan,
    max_tag_manifest_bytes: u64,
) -> Result<Vec<(String, Src<'_>)>, ExportError> {
    let mut v = vec![(
        "data/graphforge-project.json".into(),
        Src::Bytes(plan.manifest.clone()),
    )];
    v.extend(plan.files.iter().map(|f| (f.path.clone(), Src::File(f))));
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let inv = inventory(
        &v.iter()
            .map(|(p, s)| {
                (
                    p.clone(),
                    s.len(),
                    match s {
                        Src::Bytes(b) => Sha256::digest(b).into(),
                        Src::File(f) => f.digest,
                    },
                )
            })
            .collect::<Vec<_>>(),
        max_tag_manifest_bytes,
    )?;
    let tags = [
        ("bag-info.txt", BAG_INFO.to_vec()),
        ("bagit.txt", BAGIT.to_vec()),
        ("manifest-sha256.txt", inv),
    ];
    let tag = inventory(
        &tags
            .iter()
            .map(|(p, b)| (p.to_string(), b.len() as u64, Sha256::digest(b).into()))
            .collect::<Vec<_>>(),
        max_tag_manifest_bytes,
    )?;
    v.extend(tags.into_iter().map(|(p, b)| (p.into(), Src::Bytes(b))));
    v.push(("tagmanifest-sha256.txt".into(), Src::Bytes(tag)));
    Ok(v)
}
pub(super) fn bundle(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    l: PortableV2ExportLimits,
    cancelled: &impl Fn() -> bool,
    progress: &mut impl FnMut(PortableV2ExportProgress),
    allocation: &mut ExportAllocationObserver,
) -> Result<[u8; 32], ExportError> {
    let mut items = entries(plan, l.max_tag_manifest_bytes)?;
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(stage)
        .map_err(storage)?;
    allocation.register(stage, &out)?;
    let mut h = Sha256::new();
    let mut done = 0;
    for (i, (path, src)) in items.iter().enumerate() {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        observed_write_result(header(&mut out, &mut h, path, src.len()), &out, allocation)?;
        allocation.observe(&out)?;
        match src {
            Src::Bytes(b) => {
                observed_write_result(emit(&mut out, &mut h, b), &out, allocation)?;
                allocation.observe(&out)?;
            }
            Src::File(f) => stream(
                &mut out,
                &mut h,
                f,
                l.copy_buffer_bytes,
                cancelled,
                allocation,
                |n| {
                    done += n;
                    progress(PortableV2ExportProgress {
                        entries_completed: i,
                        bytes_completed: done,
                        entries_total: items.len(),
                        bytes_total: plan.payload_bytes,
                    });
                },
            )?,
        }
        observed_write_result(pad(&mut out, &mut h, src.len()), &out, allocation)?;
        allocation.observe(&out)?;
        progress(PortableV2ExportProgress {
            entries_completed: i + 1,
            bytes_completed: done,
            entries_total: items.len(),
            bytes_total: plan.payload_bytes,
        });
    }
    let end = [0u8; 1024];
    observed_write_result(out.write_all(&end).map_err(storage), &out, allocation)?;
    allocation.observe(&out)?;
    h.update(end);
    out.sync_all().map_err(storage)?;
    allocation.observe(&out)?;
    Ok(h.finalize().into())
}

fn copy(
    planned: &PlannedFile,
    target: &Path,
    size: usize,
    cancelled: &impl Fn() -> bool,
    allocation: &mut ExportAllocationObserver,
    mut tick: impl FnMut(u64),
) -> Result<(), ExportError> {
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)
        .map_err(storage)?;
    allocation.register(target, &output)?;
    if let PlannedSource::Control(bytes) = &planned.source {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        observed_write_result(
            output.write_all(bytes).map_err(storage),
            &output,
            allocation,
        )?;
        allocation.observe(&output)?;
        output.sync_all().map_err(storage)?;
        allocation.observe(&output)?;
        tick(bytes.len() as u64);
        return Ok(());
    }
    let (mut input, planned_identity) = open_planned_source(planned)?;
    let mut buffer = vec![0; size];
    let mut digest = Sha256::new();
    let mut bytes_read = 0;
    loop {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        let count = input.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        observed_write_result(
            output.write_all(&buffer[..count]).map_err(storage),
            &output,
            allocation,
        )?;
        if allocation.operation.is_some() {
            allocation.observe(&output)?;
        }
        digest.update(&buffer[..count]);
        bytes_read += count as u64;
        tick(count as u64);
    }
    output.sync_all().map_err(storage)?;
    allocation.observe(&output)?;
    if bytes_read != planned.length || <[u8; 32]>::from(digest.finalize()) != planned.digest {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    if let Some(expected) = planned_identity
        && identity(&input.metadata().map_err(storage)?)? != expected
    {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    Ok(())
}
fn stream(
    out: &mut File,
    transport: &mut Sha256,
    planned: &PlannedFile,
    size: usize,
    cancelled: &impl Fn() -> bool,
    allocation: &mut ExportAllocationObserver,
    mut tick: impl FnMut(u64),
) -> Result<(), ExportError> {
    if let PlannedSource::Control(bytes) = &planned.source {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        observed_write_result(out.write_all(bytes).map_err(storage), out, allocation)?;
        allocation.observe(out)?;
        transport.update(bytes);
        tick(bytes.len() as u64);
        return Ok(());
    }
    let (mut input, planned_identity) = open_planned_source(planned)?;
    let mut buffer = vec![0; size];
    let mut digest = Sha256::new();
    let mut bytes_read = 0;
    loop {
        if cancelled() {
            return Err(err("GF_CANCELLED", "portable export cancelled"));
        }
        let count = input.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        observed_write_result(
            out.write_all(&buffer[..count]).map_err(storage),
            out,
            allocation,
        )?;
        if allocation.operation.is_some() {
            allocation.observe(out)?;
        }
        transport.update(&buffer[..count]);
        digest.update(&buffer[..count]);
        bytes_read += count as u64;
        tick(count as u64);
    }
    if bytes_read != planned.length || <[u8; 32]>::from(digest.finalize()) != planned.digest {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    if let Some(expected) = planned_identity
        && identity(&input.metadata().map_err(storage)?)? != expected
    {
        return Err(err("GF_SOURCE_CHANGED", "source changed during export"));
    }
    Ok(())
}
pub(super) fn open_planned_source(
    planned: &PlannedFile,
) -> Result<(File, Option<Identity>), ExportError> {
    match &planned.source {
        PlannedSource::File {
            path,
            identity: expected,
        } => {
            let input = open_source_no_follow(path)?;
            if identity(&input.metadata().map_err(storage)?)? != *expected {
                return Err(err("GF_SOURCE_CHANGED", "source changed"));
            }
            Ok((input, Some(*expected)))
        }
        PlannedSource::Cas {
            lease,
            digest,
            length,
        } => {
            let source = lease
                .open(digest, *length)
                .map_err(|_| err("GF_SOURCE_CHANGED", "pinned CAS source changed"))?;
            let mut file = source.try_clone_file().map_err(storage)?;
            file.seek(SeekFrom::Start(0)).map_err(storage)?;
            Ok((file, None))
        }
        PlannedSource::Control(_) => unreachable!("control source returned above"),
    }
}

fn header(out: &mut File, h: &mut Sha256, path: &str, size: u64) -> Result<(), ExportError> {
    if let Ok((name, prefix)) = split(path) {
        return raw_header(out, h, name, prefix, size, b'0');
    }
    let suffix = &hex(Sha256::digest(path.as_bytes()).into())[..16];
    let body = pax_path_record(path);
    raw_header(
        out,
        h,
        &format!("PaxHeaders/{suffix}"),
        "",
        body.len() as u64,
        b'x',
    )?;
    emit(out, h, body.as_bytes())?;
    pad(out, h, body.len() as u64)?;
    raw_header(out, h, &format!("PaxFiles/{suffix}"), "", size, b'0')
}
fn raw_header(
    out: &mut File,
    h: &mut Sha256,
    name: &str,
    prefix: &str,
    size: u64,
    kind: u8,
) -> Result<(), ExportError> {
    let mut b = [0u8; 512];
    put(&mut b[..100], name.as_bytes());
    oct(&mut b[100..108], 0o644)?;
    oct(&mut b[108..116], 0)?;
    oct(&mut b[116..124], 0)?;
    oct(&mut b[124..136], size)?;
    oct(&mut b[136..148], 0)?;
    b[148..156].fill(b' ');
    b[156] = kind;
    put(&mut b[257..263], b"ustar\0");
    put(&mut b[263..265], b"00");
    put(&mut b[345..500], prefix.as_bytes());
    let sum: u64 = b.iter().map(|x| u64::from(*x)).sum();
    b[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    out.write_all(&b).map_err(storage)?;
    h.update(b);
    Ok(())
}
fn pax_path_record(path: &str) -> String {
    let value = format!(" path={path}\n");
    let mut digits = 1;
    loop {
        let length = digits + value.len();
        let actual_digits = length.to_string().len();
        if actual_digits == digits {
            return format!("{length}{value}");
        }
        digits = actual_digits;
    }
}
fn split(p: &str) -> Result<(&str, &str), ExportError> {
    if p.len() <= 100 {
        return Ok((p, ""));
    }
    for (i, _) in p.match_indices('/').rev() {
        let (pre, name) = p.split_at(i);
        if pre.len() <= 155 && name.len() - 1 <= 100 {
            return Ok((&name[1..], pre));
        }
    }
    Err(err("GF_INVALID_PORTABLE_PATH", "path cannot fit ustar"))
}
fn oct(dst: &mut [u8], n: u64) -> Result<(), ExportError> {
    let w = dst.len() - 1;
    let s = format!("{n:0w$o}");
    if s.len() > w {
        return Err(limit("tar field overflow"));
    }
    dst[..w].copy_from_slice(s.as_bytes());
    dst[w] = 0;
    Ok(())
}
fn put(d: &mut [u8], s: &[u8]) {
    d[..s.len()].copy_from_slice(s);
}
fn emit(o: &mut File, h: &mut Sha256, b: &[u8]) -> Result<(), ExportError> {
    o.write_all(b).map_err(storage)?;
    h.update(b);
    Ok(())
}
fn pad(output: &mut File, digest: &mut Sha256, length: u64) -> Result<(), ExportError> {
    let padding = ((512 - length % 512) % 512) as usize;
    let zeroes = [0u8; 512];
    emit(output, digest, &zeroes[..padding])
}
fn inventory(rows: &[(String, u64, [u8; 32])], limit_bytes: u64) -> Result<Vec<u8>, ExportError> {
    let mut o = Vec::new();
    for (p, _, d) in rows {
        let row_bytes = 64_u64
            .checked_add(2)
            .and_then(|value| value.checked_add(p.len() as u64))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| limit("tag inventory size overflow"))?;
        if (o.len() as u64).saturating_add(row_bytes) > limit_bytes {
            return Err(limit("tag inventory exceeds configured limit"));
        }
        o.extend(hex(*d).bytes());
        o.extend(b"  ");
        o.extend(p.bytes());
        o.push(b'\n');
    }
    Ok(o)
}
fn parent(p: &Path) -> Result<(), ExportError> {
    fs::create_dir_all(p.parent().unwrap()).map_err(storage)
}
fn write_bytes(
    root: &Path,
    p: &str,
    b: &[u8],
    allocation: &mut ExportAllocationObserver,
) -> Result<(), ExportError> {
    let p = root.join(p);
    parent(&p)?;
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&p)
        .map_err(storage)?;
    allocation.register(&p, &f)?;
    observed_write_result(f.write_all(b).map_err(storage), &f, allocation)?;
    allocation.observe(&f)?;
    f.sync_all().map_err(storage)?;
    allocation.observe(&f)
}
fn sync_tree(root: &Path) -> Result<(), ExportError> {
    let mut dirs = vec![root.into()];
    let mut i = 0;
    while i < dirs.len() {
        for e in fs::read_dir(&dirs[i]).map_err(storage)? {
            let e = e.map_err(storage)?;
            if e.file_type().map_err(storage)?.is_dir() {
                dirs.push(e.path());
            }
        }
        i += 1;
    }
    dirs.sort_by_key(|p: &PathBuf| std::cmp::Reverse(p.components().count()));
    for d in dirs {
        sync_dir(&d)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
