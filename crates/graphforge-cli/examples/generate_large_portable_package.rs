//! Stream a valid graph portable bundle of a requested size, deterministically
//! from a seed, to a file or standard output (#1777).
//!
//! The package is the fixture ontology-only package plus one `graph-data`
//! component of many graph Parquet files: each below the 64 MiB page-walk
//! threshold, and one above it so the verifier's staged-entry path runs. Files
//! are generated and hashed one at a time while they are written; the semantic
//! manifest, payload inventory and tag manifest sort after the graph tree, so
//! their digests are known when they are written. Nothing but one Parquet file
//! is ever held in memory. The method is
//! `docs/development/portable-verify-qualification.md`.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use sha2::{Digest, Sha256};

#[path = "../tests/support/portable_package.rs"]
mod portable_package;
use portable_package::{
    BAG_INFO, BAGIT, Component, DeclaredFile, controls, graph_parquet, hex, tar_header,
};

const MIB: u64 = 1024 * 1024;
const GRAPH_DIR: &str = "data/components/graph-data/graph-tree/topology/";
/// Rows of 1,150,000 bytes, one page per row. 40 rows are about 46 MB, under
/// the 64 MiB walk threshold; 64 rows are about 74 MB, over it.
const ROW_BYTES: usize = 1_150_000;
const WALKED_ROWS: usize = 40;
const STAGED_ROWS: usize = 64;
/// Room for the control entries, whose size grows with the file count.
const CONTROL_RESERVE: u64 = 4 * MIB;

#[derive(Parser)]
#[command(about = "Stream a valid graph portable bundle of a requested size")]
struct Arguments {
    /// Least size of the bundle in bytes.
    #[arg(long)]
    size_bytes: u64,
    /// Seed of the generated content; the same seed and size give the same bytes.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Output file, or `-` for standard output.
    #[arg(long, default_value = "-")]
    output: PathBuf,
}

/// The bytes of a tar entry of `length` payload bytes.
fn entry_length(length: u64) -> u64 {
    512 + length.div_ceil(512) * 512
}

fn write_entry(out: &mut impl Write, path: &str, bytes: &[u8]) -> io::Result<u64> {
    let size = bytes.len() as u64;
    out.write_all(&tar_header(path, size))?;
    out.write_all(bytes)?;
    out.write_all(&vec![0u8; (size.div_ceil(512) * 512 - size) as usize])?;
    Ok(entry_length(size))
}

fn generate(arguments: &Arguments, out: &mut impl Write) -> io::Result<u64> {
    let walked_length = entry_length(graph_parquet(WALKED_ROWS, ROW_BYTES, "").len() as u64);
    let staged_length = entry_length(graph_parquet(STAGED_ROWS, ROW_BYTES, "").len() as u64);
    let walked_files = arguments
        .size_bytes
        .saturating_sub(staged_length + CONTROL_RESERVE)
        .div_ceil(walked_length);

    let mut files = Vec::new();
    // `bag-info.txt` and `bagit.txt` sort before the graph tree and do not
    // depend on its digests; every other control sorts after it.
    let mut written = write_entry(out, "bag-info.txt", BAG_INFO)?;
    written += write_entry(out, "bagit.txt", BAGIT)?;
    for index in 0..=walked_files {
        let rows = if index == 0 { STAGED_ROWS } else { WALKED_ROWS };
        let tag = format!("{:016x}{index:08x}", arguments.seed);
        let bytes = graph_parquet(rows, ROW_BYTES, &tag);
        let path = format!("{GRAPH_DIR}nodes-{index:08}.parquet");
        written += write_entry(out, &path, &bytes)?;
        files.push(DeclaredFile {
            path,
            media_type: "application/vnd.apache.parquet",
            length: bytes.len() as u64,
            sha256: hex(&Sha256::digest(&bytes)),
        });
        if index % 100 == 0 {
            eprintln!("{index}/{walked_files} graph files, {written} bytes");
        }
    }
    let component = Component {
        kind: "graph-data",
        participant: "graph-tree",
        files,
    };
    let package = controls(std::slice::from_ref(&component), None);
    eprintln!("package_digest sha256:{}", package.package_digest);
    let mut rest: Vec<_> = package
        .entries
        .into_iter()
        .filter(|(path, _)| path != "bag-info.txt" && path != "bagit.txt")
        .collect();
    rest.sort();
    for (path, bytes) in rest {
        written += write_entry(out, &path, &bytes)?;
    }
    out.write_all(&[0u8; 1024])?;
    Ok(written + 1024)
}

fn run(arguments: &Arguments) -> io::Result<u64> {
    if arguments.output.as_os_str() == "-" {
        let mut out = BufWriter::with_capacity(MIB as usize, io::stdout().lock());
        let written = generate(arguments, &mut out)?;
        out.flush()?;
        Ok(written)
    } else {
        let mut out = BufWriter::with_capacity(MIB as usize, File::create(&arguments.output)?);
        let written = generate(arguments, &mut out)?;
        out.flush()?;
        Ok(written)
    }
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    match run(&arguments) {
        Ok(written) if written >= arguments.size_bytes => {
            eprintln!("wrote {written} bytes");
            ExitCode::SUCCESS
        }
        Ok(written) => {
            eprintln!("wrote {written} bytes, short of {}", arguments.size_bytes);
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("generate: {error}");
            ExitCode::FAILURE
        }
    }
}
