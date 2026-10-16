//! Closed presentation channel. Images never become portable Project contents.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{BufReader, Cursor};

/// Maximum explicitly selected images attached to one Version.
pub const MAX_PREVIEWS: usize = 8;
/// Maximum encoded bytes in one image.
pub const MAX_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
/// Maximum encoded bytes across one Version's image selections.
pub const MAX_TOTAL_PREVIEW_BYTES: u64 = 32 * 1024 * 1024;
/// Maximum width or height.
pub const MAX_DIMENSION: u32 = 8192;
/// Maximum decoded pixel count.
pub const MAX_PIXELS: u64 = 16_777_216;
/// Versioned discovery extension name.
pub const PREVIEW_EXTENSION: &str = "x-graphforge-previews";
/// Safe, finite refusal classifications shared by clients and Hub admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviewError {
    /// Type is outside the closed PNG/SVG channel.
    MediaType,
    /// Count, bytes, dimensions, or parser resources exceed the profile.
    Limit,
    /// Encoded image is malformed or disagrees with its declaration.
    Content,
    /// SVG contains executable, external, or unsupported content.
    UnsafeSvg,
    /// Descriptor does not name an exact immutable Version.
    Version,
}
impl std::fmt::Display for PreviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "preview.{}",
            match self {
                Self::MediaType => "media_type",
                Self::Limit => "limit",
                Self::Content => "content",
                Self::UnsafeSvg => "unsafe_svg",
                Self::Version => "version",
            }
        )
    }
}
impl std::error::Error for PreviewError {}

/// Version-bound presentation metadata; encoded images are separate objects.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewDescriptor {
    /// Exact native research Version UUID, canonical lowercase UUID form.
    pub version_uuid: String,
    /// SHA-256 of the encoded bytes.
    pub digest: String,
    /// Exactly `image/png` or `image/svg+xml`.
    pub media_type: String,
    /// Encoded byte count.
    pub length: u64,
    /// Pixel width.
    pub width: u32,
    /// Pixel height.
    pub height: u32,
    /// Optional plain-text caption, never interpreted as markup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    /// Optional accessibility text, never interpreted as markup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alt: Option<String>,
}

fn dimensions(width: u32, height: u32) -> Result<(), PreviewError> {
    if width == 0
        || height == 0
        || width > MAX_DIMENSION
        || height > MAX_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_PIXELS
    {
        return Err(PreviewError::Limit);
    }
    Ok(())
}
fn version(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        && value != "00000000-0000-0000-0000-000000000000"
}
/// Validate descriptors without fetching image bytes.
pub fn validate_descriptors(items: &[PreviewDescriptor]) -> Result<(), PreviewError> {
    if items.len() > MAX_PREVIEWS {
        return Err(PreviewError::Limit);
    }
    let mut total = 0_u64;
    let mut seen = std::collections::BTreeSet::new();
    for item in items {
        if !version(&item.version_uuid) {
            return Err(PreviewError::Version);
        }
        if !matches!(item.media_type.as_str(), "image/png" | "image/svg+xml") {
            return Err(PreviewError::MediaType);
        }
        if item.length == 0 || item.length > MAX_PREVIEW_BYTES as u64 {
            return Err(PreviewError::Limit);
        }
        total = total.checked_add(item.length).ok_or(PreviewError::Limit)?;
        if total > MAX_TOTAL_PREVIEW_BYTES {
            return Err(PreviewError::Limit);
        }
        dimensions(item.width, item.height)?;
        if item.digest.len() != 71
            || !item.digest.starts_with("sha256:")
            || !item.digest[7..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !seen.insert((&item.version_uuid, &item.digest))
        {
            return Err(PreviewError::Content);
        }
        for (text, max) in [(&item.caption, 1024), (&item.alt, 2048)] {
            if text.as_ref().is_some_and(|s| {
                s.len() > max || s.chars().any(|c| c.is_control() && c != '\n' && c != '\t')
            }) {
                return Err(PreviewError::Limit);
            }
        }
    }
    Ok(())
}
/// Validate bytes and return their authoritative pixel dimensions.
pub fn validate_image(media_type: &str, bytes: &[u8]) -> Result<(u32, u32), PreviewError> {
    if bytes.is_empty() || bytes.len() > MAX_PREVIEW_BYTES {
        return Err(PreviewError::Limit);
    }
    match media_type {
        "image/png" => png_dimensions(bytes),
        "image/svg+xml" => svg_dimensions(bytes),
        _ => Err(PreviewError::MediaType),
    }
}
/// Revalidate untrusted bytes against every declared field at Hub commit.
pub fn validate_preview(item: &PreviewDescriptor, bytes: &[u8]) -> Result<(), PreviewError> {
    validate_descriptors(std::slice::from_ref(item))?;
    if bytes.len() as u64 != item.length || digest(bytes) != item.digest {
        return Err(PreviewError::Content);
    }
    if validate_image(&item.media_type, bytes)? != (item.width, item.height) {
        return Err(PreviewError::Content);
    }
    Ok(())
}
fn png_dimensions(bytes: &[u8]) -> Result<(u32, u32), PreviewError> {
    png_framing(bytes)?;
    let mut options = png::DecodeOptions::default();
    options.set_ignore_checksums(false);
    options.set_skip_ancillary_crc_failures(false);
    let mut decoder = png::Decoder::new_with_options(BufReader::new(Cursor::new(bytes)), options);
    decoder.set_limits(png::Limits {
        bytes: 2 * 1024 * 1024,
    });
    let mut reader = decoder.read_info().map_err(|_| PreviewError::Content)?;
    let (w, h) = (reader.info().width, reader.info().height);
    dimensions(w, h)?;
    if reader.info().animation_control.is_some() {
        return Err(PreviewError::Content);
    }
    while reader
        .next_row()
        .map_err(|_| PreviewError::Content)?
        .is_some()
    {}
    reader.finish().map_err(|_| PreviewError::Content)?;
    Ok((w, h))
}
fn svg_dimensions(bytes: &[u8]) -> Result<(u32, u32), PreviewError> {
    svg_resource_preflight(bytes)?;
    let text = std::str::from_utf8(bytes).map_err(|_| PreviewError::Content)?;
    let doc = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 100_000,
            ..Default::default()
        },
    )
    .map_err(|_| PreviewError::UnsafeSvg)?;
    let root = doc.root_element();
    if root.tag_name().name() != "svg"
        || root.tag_name().namespace() != Some("http://www.w3.org/2000/svg")
    {
        return Err(PreviewError::UnsafeSvg);
    }
    let number = |value: Option<&str>| -> Result<u32, PreviewError> {
        let value = value.ok_or(PreviewError::Content)?;
        let value = value.strip_suffix("px").unwrap_or(value);
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(PreviewError::Content);
        }
        value.parse().map_err(|_| PreviewError::Limit)
    };
    let (w, h) = (
        number(root.attribute("width"))?,
        number(root.attribute("height"))?,
    );
    dimensions(w, h)?;
    for node in doc.descendants() {
        if node.ancestors().count() > 64 || node.is_pi() {
            return Err(PreviewError::UnsafeSvg);
        }
        if !node.is_element() {
            continue;
        }
        if node.tag_name().namespace() != Some("http://www.w3.org/2000/svg")
            || !matches!(
                node.tag_name().name(),
                "svg"
                    | "g"
                    | "defs"
                    | "path"
                    | "rect"
                    | "circle"
                    | "ellipse"
                    | "line"
                    | "polyline"
                    | "polygon"
                    | "text"
                    | "tspan"
                    | "title"
                    | "desc"
                    | "clipPath"
                    | "linearGradient"
                    | "radialGradient"
                    | "stop"
            )
        {
            return Err(PreviewError::UnsafeSvg);
        }
        if node.attributes().len() > 64 {
            return Err(PreviewError::Limit);
        }
        validate_svg_attributes(node)?;
    }
    Ok((w, h))
}

// Reject polyglot/trailing streams and bound ancillary data before decoder allocation.
fn png_framing(bytes: &[u8]) -> Result<(), PreviewError> {
    if !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Err(PreviewError::Content);
    }
    let mut at = 8_usize;
    let mut chunks = 0_usize;
    while at < bytes.len() {
        let header = bytes.get(at..at + 8).ok_or(PreviewError::Content)?;
        let length =
            u32::from_be_bytes(header[..4].try_into().map_err(|_| PreviewError::Content)?) as usize;
        let kind = &header[4..8];
        chunks += 1;
        if chunks > 100_000 {
            return Err(PreviewError::Limit);
        }
        // No text/ICC/EXIF decompression or animation in this closed channel.
        if !matches!(
            kind,
            b"IHDR" | b"PLTE" | b"IDAT" | b"IEND" | b"tRNS" | b"sRGB" | b"gAMA" | b"cHRM" | b"pHYs"
        ) {
            return Err(PreviewError::Content);
        }
        at = at
            .checked_add(length)
            .and_then(|n| n.checked_add(12))
            .ok_or(PreviewError::Limit)?;
        if at > bytes.len() {
            return Err(PreviewError::Content);
        }
        if kind == b"IEND" {
            return if length == 0 && at == bytes.len() {
                Ok(())
            } else {
                Err(PreviewError::Content)
            };
        }
    }
    Err(PreviewError::Content)
}

/// Canonical content identity of encoded preview bytes.
#[must_use]
pub fn digest(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(output, "{byte:02x}").expect("string write");
    }
    output
}
/// Versioned typed discovery extension. Absence means no previews selected.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewSet {
    /// Must equal `graphforge-previews/1`.
    pub format: String,
    /// Explicitly selected Version-bound images.
    pub previews: Vec<PreviewDescriptor>,
}
impl PreviewSet {
    /// Validate format and bounded metadata.
    pub fn validate(&self) -> Result<(), PreviewError> {
        if self.format != "graphforge-previews/1" {
            return Err(PreviewError::Version);
        }
        validate_descriptors(&self.previews)
    }
}

/// Deterministic descriptor schema; content and cross-object/Version bindings
/// additionally require the Rust admission API.
#[must_use]
pub fn descriptor_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "title":"GraphForge Version previews v1","type":"object","additionalProperties":false,
        "required":["format","previews"],"properties":{
            "format":{"const":"graphforge-previews/1"},
            "previews":{"type":"array","maxItems":MAX_PREVIEWS,"items":{
                "type":"object","additionalProperties":false,
                "required":["version_uuid","digest","media_type","length","width","height"],
                "properties":{
                    "version_uuid":{"type":"string","pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"},
                    "digest":{"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
                    "media_type":{"enum":["image/png","image/svg+xml"]},
                    "length":{"type":"integer","minimum":1,"maximum":MAX_PREVIEW_BYTES},
                    "width":{"type":"integer","minimum":1,"maximum":MAX_DIMENSION},
                    "height":{"type":"integer","minimum":1,"maximum":MAX_DIMENSION},
                    "caption":{"type":["string","null"],"maxLength":1024},
                    "alt":{"type":["string","null"],"maxLength":2048}
                }
            }}
        }
    })
}

fn validate_svg_attributes(node: roxmltree::Node<'_, '_>) -> Result<(), PreviewError> {
    for attr in node.attributes() {
        if attr.namespace().is_some() {
            return Err(PreviewError::UnsafeSvg);
        }
        if !matches!(
            attr.name(),
            "id" | "width"
                | "height"
                | "viewBox"
                | "preserveAspectRatio"
                | "x"
                | "y"
                | "x1"
                | "x2"
                | "y1"
                | "y2"
                | "cx"
                | "cy"
                | "r"
                | "rx"
                | "ry"
                | "d"
                | "points"
                | "transform"
                | "fill"
                | "fill-opacity"
                | "fill-rule"
                | "stroke"
                | "stroke-width"
                | "stroke-opacity"
                | "stroke-linecap"
                | "stroke-linejoin"
                | "stroke-dasharray"
                | "stroke-dashoffset"
                | "opacity"
                | "clip-path"
                | "clip-rule"
                | "offset"
                | "stop-color"
                | "stop-opacity"
                | "gradientUnits"
                | "gradientTransform"
                | "spreadMethod"
                | "href"
                | "font-family"
                | "font-size"
                | "font-weight"
                | "font-style"
                | "text-anchor"
                | "dominant-baseline"
                | "dx"
                | "dy"
                | "rotate"
                | "textLength"
                | "lengthAdjust"
        ) {
            return Err(PreviewError::UnsafeSvg);
        }
        let value = attr.value();
        // Closed lexical profile: no CSS escapes/comments/control bytes or
        // indirection that can disguise a resource URL from this reader.
        if value.contains(['\\', '&', '<', '>'])
            || value.contains("/*")
            || value.chars().any(char::is_control)
        {
            return Err(PreviewError::UnsafeSvg);
        }
        if attr.name() == "href"
            && (!value.starts_with('#')
                || value.len() < 2
                || !value[1..]
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b)))
        {
            return Err(PreviewError::UnsafeSvg);
        }
        if value.to_ascii_lowercase().contains("url") {
            let Some(id) = value
                .strip_prefix("url(#")
                .and_then(|v| v.strip_suffix(')'))
            else {
                return Err(PreviewError::UnsafeSvg);
            };
            if id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
            {
                return Err(PreviewError::UnsafeSvg);
            }
        }
    }
    Ok(())
}

// roxmltree reserves from raw '<'/'=' counts and resolves duplicate attributes
// before exposing nodes. Bound those allocations and per-element quadratic
// work BEFORE building its DOM, using a standard borrowing XML tokenizer.
fn svg_resource_preflight(bytes: &[u8]) -> Result<(), PreviewError> {
    use quick_xml::events::Event;
    let mut openings = 0_u32;
    let mut equals = 0_u32;
    for byte in bytes {
        match byte {
            b'<' => openings += 1,
            b'=' => equals += 1,
            _ => {}
        }
        if openings > 100_000 || equals > 100_000 {
            return Err(PreviewError::Limit);
        }
    }
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0_usize;
    let mut nodes = 0_usize;
    loop {
        let event = reader.read_event().map_err(|_| PreviewError::UnsafeSvg)?;
        nodes += 1;
        if nodes > 100_000 {
            return Err(PreviewError::Limit);
        }
        match event {
            Event::Eof => break,
            Event::DocType(_) | Event::PI(_) => return Err(PreviewError::UnsafeSvg),
            Event::Start(ref element) | Event::Empty(ref element) => {
                // Disable tokenizer duplicate-name scans; DOM validation will
                // do that only after this small per-element bound is proven.
                let mut count = 0;
                for attr in element.attributes().with_checks(false) {
                    attr.map_err(|_| PreviewError::UnsafeSvg)?;
                    count += 1;
                    if count > 64 {
                        return Err(PreviewError::Limit);
                    }
                }
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                }
                if depth > 62 {
                    return Err(PreviewError::Limit);
                }
            }
            Event::End(_) => {
                depth = depth.checked_sub(1).ok_or(PreviewError::UnsafeSvg)?;
            }
            _ => {}
        }
    }
    Ok(())
}
