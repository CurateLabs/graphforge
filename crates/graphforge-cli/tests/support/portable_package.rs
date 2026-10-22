//! A valid portable bundle built from declared components (#1777).
//!
//! The fixture ontology-only package plus any number of components, with the
//! manifest, `package_digest`, payload inventory and tag manifest rebuilt to
//! describe them. Shared by the stdin verification tests and the large-package
//! generator example; neither holds a package's payload here, only its declared
//! length and digest.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const ONTOLOGY_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
pub const MANIFEST_PATH: &str = "data/graphforge-project.json";
pub const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
pub const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable\nBagging-Date: 1970-01-01\n";

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// RFC 8785-shaped canonical JSON for the strings, integers and containers a
/// manifest holds: sorted keys, no whitespace.
pub fn canonical(value: &Value) -> Vec<u8> {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map.iter().collect();
            let mut out = vec![b'{'];
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend(serde_json::to_vec(key).unwrap());
                out.push(b':');
                out.extend(canonical(value));
            }
            out.push(b'}');
            out
        }
        Value::Array(values) => {
            let mut out = vec![b'['];
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend(canonical(value));
            }
            out.push(b']');
            out
        }
        scalar => serde_json::to_vec(scalar).unwrap(),
    }
}

fn octal(field: &mut [u8], value: u64) {
    field.fill(0);
    let digits = format!("{:0width$o}", value, width = field.len() - 1);
    field[..digits.len()].copy_from_slice(digits.as_bytes());
}

/// The canonical ustar header of a regular entry of `size` bytes at `path`
/// (every path here fits the 100-byte name field).
pub fn tar_header(path: &str, size: u64) -> Vec<u8> {
    assert!(path.len() <= 100);
    let mut header = [0u8; 512];
    header[..path.len()].copy_from_slice(path.as_bytes());
    octal(&mut header[100..108], 0o644);
    octal(&mut header[108..116], 0);
    octal(&mut header[116..124], 0);
    octal(&mut header[124..136], size);
    octal(&mut header[136..148], 0);
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let sum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
    header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    header.to_vec()
}

/// One file of a component, as the manifest declares it.
pub struct DeclaredFile {
    pub path: String,
    pub media_type: &'static str,
    pub length: u64,
    /// SHA-256 of the file, lower-case hex.
    pub sha256: String,
}

/// A component added to the fixture package: the files of one `kind`.
pub struct Component {
    pub kind: &'static str,
    pub participant: &'static str,
    pub files: Vec<DeclaredFile>,
}

/// The package-control entries that describe `components`, and the package's
/// digest.
pub struct Controls {
    /// `bag-info.txt`, `bagit.txt`, the ontology, the semantic manifest, the
    /// payload inventory and the tag manifest, unordered.
    pub entries: Vec<(String, Vec<u8>)>,
    pub package_digest: String,
}

/// The controls of the fixture ontology-only package plus `components`.
/// `manifest` replaces the semantic manifest bytes when given (an impostor),
/// with the inventories describing it.
pub fn controls(components: &[Component], manifest: Option<Vec<u8>>) -> Controls {
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let object = document.as_object_mut().unwrap();
    object.remove("package_digest");
    object.insert("package_class".into(), "component-selective".into());
    let mut capabilities = vec!["ontology@1".to_owned()];
    let mut roots = vec!["core-ontology".to_owned()];
    let mut declared = document["components"].as_array().unwrap().clone();
    for component in components {
        capabilities.push(format!("{}@1", component.kind));
        roots.push(component.participant.to_owned());
        let mut files: Vec<_> = component.files.iter().collect();
        files.sort_by(|left, right| left.path.cmp(&right.path));
        declared.push(serde_json::json!({
            "kind": component.kind,
            "files": files.iter().map(|file| serde_json::json!({
                "length": file.length,
                "media_type": file.media_type,
                "path": file.path,
                "sha256": file.sha256,
            })).collect::<Vec<_>>(),
            "participant_id": component.participant,
            "required_dependencies": [],
        }));
    }
    capabilities.sort();
    roots.sort();
    declared.sort_by_key(|component| {
        (
            component["kind"].as_str().unwrap().to_owned(),
            component["participant_id"].as_str().unwrap().to_owned(),
        )
    });
    document["requirements"]["capabilities"] = capabilities.into();
    document["selection"]["roots"] = roots.into();
    document["components"] = declared.into();
    let package_digest = hex(&Sha256::digest(
        [b"graphforge-project/2\0".as_slice(), &canonical(&document)].concat(),
    ));
    document["package_digest"] = format!("sha256:{package_digest}").into();
    let manifest = manifest.unwrap_or_else(|| canonical(&document));

    let mut data = BTreeMap::from([
        (ONTOLOGY_PATH.to_owned(), hex(&Sha256::digest(b"{}"))),
        (MANIFEST_PATH.to_owned(), hex(&Sha256::digest(&manifest))),
    ]);
    for file in components.iter().flat_map(|component| &component.files) {
        data.insert(file.path.clone(), file.sha256.clone());
    }
    let inventory: String = data
        .iter()
        .map(|(path, digest)| format!("{digest}  {path}\n"))
        .collect();
    let tags = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(&Sha256::digest(BAG_INFO)),
        hex(&Sha256::digest(BAGIT)),
        hex(&Sha256::digest(inventory.as_bytes()))
    );
    Controls {
        entries: vec![
            ("bag-info.txt".into(), BAG_INFO.to_vec()),
            ("bagit.txt".into(), BAGIT.to_vec()),
            (ONTOLOGY_PATH.into(), b"{}".to_vec()),
            (MANIFEST_PATH.into(), manifest),
            ("manifest-sha256.txt".into(), inventory.into()),
            ("tagmanifest-sha256.txt".into(), tags.into()),
        ],
        package_digest,
    }
}

/// A valid graph Parquet file: plain-encoded, uncompressed, one page per row,
/// so it passes the encoding profile and is walked. Each of `rows` values is
/// `row_bytes` long and begins with `tag` and the row number, so files with
/// different tags differ in every page.
pub fn graph_parquet(rows: usize, row_bytes: usize, tag: &str) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let values = StringArray::from_iter_values((0..rows).map(|row| {
        let mut value = format!("{tag}{row:06}");
        value.push_str(&"x".repeat(row_bytes - value.len()));
        value
    }));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_write_batch_size(1)
        .set_data_page_row_count_limit(1)
        .set_max_row_group_row_count(Some(rows))
        .build();
    let mut file = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

const LARGE_GRAPH_DIR: &str = "data/components/graph-data/graph-tree/topology/";
/// Rows of 1,150,000 bytes, one page per row. 40 rows are about 46 MB, under
/// the 64 MiB walk threshold; 64 rows are about 74 MB, over it.
const LARGE_ROW_BYTES: usize = 1_150_000;
const WALKED_ROWS: usize = 40;
const STAGED_ROWS: usize = 64;

/// The bytes a tar entry of `length` payload bytes takes: header, payload and
/// padding.
pub fn entry_length(length: u64) -> u64 {
    512 + length.div_ceil(512) * 512
}

fn write_entry(out: &mut impl Write, path: &str, bytes: &[u8]) -> io::Result<u64> {
    let size = bytes.len() as u64;
    out.write_all(&tar_header(path, size))?;
    out.write_all(bytes)?;
    out.write_all(&vec![0u8; (size.div_ceil(512) * 512 - size) as usize])?;
    Ok(entry_length(size))
}

/// The bytes the graph entry over the page-walk threshold takes in a bundle.
pub fn staged_entry_length() -> u64 {
    entry_length(graph_parquet(STAGED_ROWS, LARGE_ROW_BYTES, "").len() as u64)
}

/// A graph bundle written by [`write_large_package`].
pub struct LargePackage {
    pub package_digest: String,
    /// Bytes written, controls and end-of-archive blocks included.
    pub written: u64,
}

/// Stream a valid graph bundle of at least `size_bytes` to `out`,
/// deterministically from `seed`, holding one Parquet file at a time: a graph
/// tree of one file over the 64 MiB page-walk threshold and as many files under
/// it as `size_bytes` needs. The graph entries alone reach `size_bytes`, so the
/// controls only add to it. The semantic manifest, payload inventory and tag
/// manifest sort after the graph tree, so their digests are known when they are
/// written. `progress` is called with the graph files written so far and their
/// total.
pub fn write_large_package(
    size_bytes: u64,
    seed: u64,
    out: &mut impl Write,
    mut progress: impl FnMut(u64, u64, u64),
) -> io::Result<LargePackage> {
    let walked_length = entry_length(graph_parquet(WALKED_ROWS, LARGE_ROW_BYTES, "").len() as u64);
    let walked_files = size_bytes
        .saturating_sub(staged_entry_length())
        .div_ceil(walked_length);

    let mut files = Vec::new();
    // `bag-info.txt` and `bagit.txt` sort before the graph tree and do not
    // depend on its digests; every other control sorts after it.
    let mut written = write_entry(out, "bag-info.txt", BAG_INFO)?;
    written += write_entry(out, "bagit.txt", BAGIT)?;
    for index in 0..=walked_files {
        let rows = if index == 0 { STAGED_ROWS } else { WALKED_ROWS };
        let tag = format!("{seed:016x}{index:08x}");
        let bytes = graph_parquet(rows, LARGE_ROW_BYTES, &tag);
        let path = format!("{LARGE_GRAPH_DIR}nodes-{index:08}.parquet");
        written += write_entry(out, &path, &bytes)?;
        files.push(DeclaredFile {
            path,
            media_type: "application/vnd.apache.parquet",
            length: bytes.len() as u64,
            sha256: hex(&Sha256::digest(&bytes)),
        });
        progress(index, walked_files, written);
    }
    let component = Component {
        kind: "graph-data",
        participant: "graph-tree",
        files,
    };
    let package = controls(std::slice::from_ref(&component), None);
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
    Ok(LargePackage {
        package_digest: package.package_digest,
        written: written + 1024,
    })
}
