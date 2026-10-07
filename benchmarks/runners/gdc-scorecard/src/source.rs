//! Input readers. Every format yields Utf8 record batches whose column names
//! come from the file header (LDBC CSV) or are fixed by the format
//! (Graphalytics), so the converter parses values in one place.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{Cause, ConvertError, io_error};
use crate::mapping::Format;

pub const BATCH_ROWS: usize = 65_536;

/// Reads `path`, checks that `required` columns exist, and calls `visit` with
/// each batch and the 1-based data-row number of its first row.
///
/// # Errors
/// `input_missing`, `malformed_input`, `missing_column`, or whatever `visit`
/// returns.
pub fn read_table(
    format: Format,
    path: &Path,
    required: &[&str],
    visit: &mut dyn FnMut(&RecordBatch, u64) -> Result<(), ConvertError>,
) -> Result<(), ConvertError> {
    let file = File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ConvertError::new(Cause::InputMissing, format!("{}", path.display()))
        } else {
            io_error(&path.display().to_string(), &error)
        }
    })?;
    // Spark writes one gzip member per part file; a multi-member reader also
    // accepts concatenated members rather than stopping after the first.
    let decoded: Box<dyn Read> = if is_gzip(path) {
        Box::new(flate2::read::MultiGzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(file)
    };
    let reader = BufReader::with_capacity(1 << 20, decoded);
    match format {
        Format::LdbcCsv => read_ldbc_csv(reader, path, required, visit),
        Format::GraphalyticsVertices | Format::GraphalyticsEdges => {
            read_graphalytics(reader, format, path, required, visit)
        }
    }
}

fn check_required(header: &[String], required: &[&str], path: &Path) -> Result<(), ConvertError> {
    for column in required {
        if !header.iter().any(|name| name == column) {
            return Err(ConvertError::new(
                Cause::MissingColumn,
                format!("{}: column {column} not found", path.display()),
            ));
        }
    }
    Ok(())
}

fn utf8_schema(header: &[String]) -> Arc<Schema> {
    Arc::new(Schema::new(
        header
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

fn is_gzip(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "gz")
}

/// Undecodable bytes (bad gzip, invalid UTF-8) are malformed input, not an
/// I/O failure of the host.
fn read_error(path: &Path, error: &std::io::Error) -> ConvertError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::UnexpectedEof
    ) {
        ConvertError::new(
            Cause::MalformedInput,
            format!("{}: {error}", path.display()),
        )
    } else {
        io_error(&path.display().to_string(), error)
    }
}

/// Names a repeated column `name.1`, `name.2`, ... on its second, third, ...
/// occurrence. A suffixed name that collides with another column is refused,
/// so every column keeps exactly one name.
fn disambiguate(header: Vec<String>, path: &Path) -> Result<Vec<String>, ConvertError> {
    let mut seen: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    let mut named = Vec::with_capacity(header.len());
    for name in header {
        let repeats = seen.entry(name.clone()).or_insert(0);
        named.push(if *repeats == 0 {
            name
        } else {
            format!("{name}.{repeats}")
        });
        *repeats += 1;
    }
    let unique: BTreeSet<&String> = named.iter().collect();
    if unique.len() != named.len() {
        return Err(ConvertError::new(
            Cause::MalformedInput,
            format!(
                "{}: header repeats a column name that its .N suffix cannot disambiguate",
                path.display()
            ),
        ));
    }
    Ok(named)
}

fn read_ldbc_csv(
    mut reader: BufReader<Box<dyn Read>>,
    path: &Path,
    required: &[&str],
    visit: &mut dyn FnMut(&RecordBatch, u64) -> Result<(), ConvertError>,
) -> Result<(), ConvertError> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| read_error(path, &error))?;
    let header: Vec<String> = line
        .trim_end_matches(['\n', '\r'])
        .split('|')
        .map(str::to_owned)
        .collect();
    if header.iter().any(String::is_empty) {
        return Err(ConvertError::new(
            Cause::MalformedInput,
            format!(
                "{}: header is empty or has an empty column name",
                path.display()
            ),
        ));
    }
    let header = disambiguate(header, path)?;
    check_required(&header, required, path)?;
    // LDBC CSV is never quoted. A control byte that cannot occur in the data
    // disables arrow-csv's quote handling so a stray `"` stays literal.
    let csv = arrow::csv::ReaderBuilder::new(utf8_schema(&header))
        .with_delimiter(b'|')
        .with_header(false)
        .with_quote(0x01)
        .with_batch_size(BATCH_ROWS)
        .build_buffered(reader)
        .map_err(|error| {
            ConvertError::new(
                Cause::MalformedInput,
                format!("{}: {error}", path.display()),
            )
        })?;
    let mut next_row = 1_u64;
    for batch in csv {
        let batch = batch.map_err(|error| {
            ConvertError::new(
                Cause::MalformedInput,
                format!("{} near row {next_row}: {error}", path.display()),
            )
        })?;
        visit(&batch, next_row)?;
        next_row += batch.num_rows() as u64;
    }
    Ok(())
}

fn read_graphalytics(
    reader: BufReader<Box<dyn Read>>,
    format: Format,
    path: &Path,
    required: &[&str],
    visit: &mut dyn FnMut(&RecordBatch, u64) -> Result<(), ConvertError>,
) -> Result<(), ConvertError> {
    let mut lines = reader.lines();
    let mut pending: Option<String> = None;
    let mut header: Vec<String> = Vec::new();
    let mut width = 0;
    for line in lines.by_ref() {
        let line = line.map_err(|error| read_error(path, &error))?;
        if line.trim().is_empty() {
            continue;
        }
        width = line.split_ascii_whitespace().count();
        header = match (format, width) {
            (Format::GraphalyticsVertices, 1) => vec!["id".to_owned()],
            (Format::GraphalyticsEdges, 2) => vec!["source".to_owned(), "target".to_owned()],
            (Format::GraphalyticsEdges, 3) => {
                vec![
                    "source".to_owned(),
                    "target".to_owned(),
                    "weight".to_owned(),
                ]
            }
            _ => {
                return Err(ConvertError::new(
                    Cause::MalformedInput,
                    format!("{}: first line has {width} fields", path.display()),
                ));
            }
        };
        pending = Some(line);
        break;
    }
    if header.is_empty() {
        header = match format {
            Format::GraphalyticsVertices => vec!["id".to_owned()],
            _ => vec!["source".to_owned(), "target".to_owned()],
        };
        width = header.len();
    }
    check_required(&header, required, path)?;
    let schema = utf8_schema(&header);
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); width];
    let mut rows_in_batch = 0_usize;
    let mut first_row = 1_u64;
    let mut ordinal = 0_u64;
    let flush = |columns: &mut Vec<Vec<String>>,
                 first_row: u64,
                 visit: &mut dyn FnMut(&RecordBatch, u64) -> Result<(), ConvertError>|
     -> Result<(), ConvertError> {
        let arrays: Vec<ArrayRef> = columns
            .iter_mut()
            .map(|values| Arc::new(StringArray::from(std::mem::take(values))) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|error| ConvertError::new(Cause::MalformedInput, error.to_string()))?;
        visit(&batch, first_row)
    };
    for line in pending.map(Ok).into_iter().chain(lines) {
        let line = line.map_err(|error| read_error(path, &error))?;
        if line.trim().is_empty() {
            continue;
        }
        ordinal += 1;
        let mut fields = line.split_ascii_whitespace();
        let mut count = 0;
        for column in columns.iter_mut() {
            let Some(value) = fields.next() else { break };
            column.push(value.to_owned());
            count += 1;
        }
        if count != width || fields.next().is_some() {
            return Err(ConvertError::new(
                Cause::MalformedInput,
                format!("{} row {ordinal}: expected {width} fields", path.display()),
            ));
        }
        rows_in_batch += 1;
        if rows_in_batch == BATCH_ROWS {
            flush(&mut columns, first_row, visit)?;
            first_row = ordinal + 1;
            rows_in_batch = 0;
        }
    }
    if rows_in_batch > 0 {
        flush(&mut columns, first_row, visit)?;
    }
    Ok(())
}
