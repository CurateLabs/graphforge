//! Stream a valid graph portable bundle of a requested size, deterministically
//! from a seed, to a file or standard output (#1777).
//!
//! The package is the fixture ontology-only package plus one `graph-data`
//! component of many graph Parquet files: each below the 64 MiB page-walk
//! threshold, and one above it so the verifier's staged-entry path runs. Nothing
//! but one Parquet file is ever held in memory. The writer is
//! `tests/support/portable_package.rs`; the method is
//! `docs/development/portable-verify-qualification.md`.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[path = "../tests/support/portable_package.rs"]
mod portable_package;
use portable_package::write_large_package;

const MIB: usize = 1024 * 1024;

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

fn generate(arguments: &Arguments, out: &mut impl Write) -> io::Result<()> {
    let package = write_large_package(
        arguments.size_bytes,
        arguments.seed,
        out,
        |index, files, written| {
            if index % 100 == 0 {
                eprintln!("{index}/{files} graph files, {written} bytes");
            }
        },
    )?;
    out.flush()?;
    eprintln!("package_digest sha256:{}", package.package_digest);
    eprintln!("wrote {} bytes", package.written);
    Ok(())
}

fn run(arguments: &Arguments) -> io::Result<()> {
    if arguments.output.as_os_str() == "-" {
        generate(
            arguments,
            &mut BufWriter::with_capacity(MIB, io::stdout().lock()),
        )
    } else {
        generate(
            arguments,
            &mut BufWriter::with_capacity(MIB, File::create(&arguments.output)?),
        )
    }
}

fn main() -> ExitCode {
    match run(&Arguments::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("generate: {error}");
            ExitCode::FAILURE
        }
    }
}
