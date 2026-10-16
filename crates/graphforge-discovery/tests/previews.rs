//! Native image admission, resource, and deterministic corpus regressions.
use graphforge_discovery::preview::*;
use serde::Deserialize;
use std::fmt::Write as _;

fn png(width: u32, height: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, width, height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&vec![0; (width * height * 4) as usize])
            .unwrap();
    }
    bytes
}
fn svg(content: &str) -> Vec<u8> {
    format!(r#"<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16">{content}</svg>"#)
        .into_bytes()
}
fn descriptor(bytes: &[u8], media: &str) -> PreviewDescriptor {
    let (width, height) = validate_image(media, bytes).unwrap();
    PreviewDescriptor {
        version_uuid: "01990000-0000-7000-8000-000000000001".into(),
        digest: digest(bytes),
        media_type: media.into(),
        length: bytes.len() as u64,
        width,
        height,
        caption: None,
        alt: None,
    }
}
#[test]
fn full_png_decode_crc_end_and_closed_metadata_are_required() {
    let bytes = png(2, 3);
    assert_eq!(validate_image("image/png", &bytes), Ok((2, 3)));
    let mut bad = bytes.clone();
    bad[29] ^= 1;
    assert_eq!(
        validate_image("image/png", &bad),
        Err(PreviewError::Content)
    );
    assert!(validate_image("image/png", &bytes[..bytes.len() - 12]).is_err());
    let mut trailing = bytes.clone();
    trailing.extend(&bytes);
    assert!(validate_image("image/png", &trailing).is_err());
    // Valid PNG encoder with compressed text is excluded before decompression.
    let mut text = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut text, 1, 1);
        encoder
            .add_ztxt_chunk("k".into(), "x".repeat(1_000_000))
            .unwrap();
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&[0]).unwrap();
    }
    assert!(validate_image("image/png", &text).is_err());
    assert_eq!(
        validate_image("text/html", b"<html/>"),
        Err(PreviewError::MediaType)
    );
    assert_eq!(
        validate_image("image/png", &vec![0; MAX_PREVIEW_BYTES + 1]),
        Err(PreviewError::Limit)
    );
}
#[test]
fn descriptors_bind_bytes_dimensions_version_and_text_limits() {
    let bytes = png(2, 3);
    let item = descriptor(&bytes, "image/png");
    validate_preview(&item, &bytes).unwrap();
    for mutate in [0, 1, 2, 3, 4] {
        let mut bad = item.clone();
        match mutate {
            0 => bad.digest.replace_range(7..8, "f"),
            1 => bad.length += 1,
            2 => bad.media_type = "image/svg+xml".into(),
            3 => bad.width += 1,
            _ => bad.version_uuid = "bad".into(),
        }
        assert!(validate_preview(&bad, &bytes).is_err());
    }
    let mut bad = item.clone();
    bad.version_uuid = "00000000-0000-0000-0000-000000000000".into();
    assert_eq!(validate_descriptors(&[bad]), Err(PreviewError::Version));
    let mut bad = item.clone();
    bad.alt = Some("é".repeat(1025));
    assert_eq!(validate_descriptors(&[bad]), Err(PreviewError::Limit));
    assert!(validate_descriptors(&[item.clone(), item.clone()]).is_err());
    let make = |count: usize| {
        (0..count)
            .map(|n| {
                let mut d = item.clone();
                d.digest = format!("sha256:{n:064x}");
                d.length = 4 * 1024 * 1024;
                d
            })
            .collect::<Vec<_>>()
    };
    assert!(validate_descriptors(&make(8)).is_ok());
    assert!(validate_descriptors(&make(9)).is_err());
    let mut over = make(8);
    over[0].length += 1;
    assert!(validate_descriptors(&over).is_err());
    let mut dimensions = item;
    dimensions.width = 8192;
    dimensions.height = 2048;
    assert!(validate_descriptors(&[dimensions.clone()]).is_ok());
    dimensions.height += 1;
    assert!(validate_descriptors(&[dimensions]).is_err());
}
#[derive(Deserialize)]
struct Case {
    name: String,
    media_type: String,
    utf8: Option<String>,
    hex: Option<String>,
    error: Option<PreviewError>,
}
#[test]
fn deterministic_preview_corpus() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../../docs/reference/hub-publish/v1/previews.json"
    ))
    .unwrap();
    for case in cases {
        let bytes = case.utf8.map_or_else(
            || {
                let h = case.hex.unwrap();
                (0..h.len())
                    .step_by(2)
                    .map(|n| u8::from_str_radix(&h[n..n + 2], 16).unwrap())
                    .collect()
            },
            String::into_bytes,
        );
        let result = validate_image(&case.media_type, &bytes);
        assert_eq!(result.err(), case.error, "{}", case.name);
    }
}
#[test]
fn svg_closed_profile_rejects_dtd_namespaces_css_and_resource_cycles() {
    for body in [
        r"<script/>",
        r"<foreignObject/>",
        r"<style>path{fill:red}</style>",
        r#"<path style="fill:red"/>"#,
        r#"<path OnLoad="x"/>"#,
        r##"<use href="#a"/>"##,
        r#"<path fill="url(https://evil.test/x)"/>"#,
        r#"<path fill="u\72l(#a)"/>"#,
        r#"<g xml:base="https://evil.test"><path/></g>"#,
        r#"<x:path xmlns:x="https://evil.test"/>"#,
        r"<animate/>",
    ] {
        assert_eq!(
            validate_image("image/svg+xml", &svg(body)),
            Err(PreviewError::UnsafeSvg),
            "{body}"
        );
    }
    let dtd=b"<!DOCTYPE svg [<!ENTITY x 'hello'>]><svg xmlns='http://www.w3.org/2000/svg' width='1' height='1'>&x;</svg>";
    assert!(validate_image("image/svg+xml", dtd).is_err());
    assert_eq!(
        validate_image(
            "image/svg+xml",
            &svg(
                r#"<defs><clipPath id="a"><rect width="2" height="2"/></clipPath></defs><path clip-path="url(#a)" d="M0 0L1 1"/>"#
            )
        ),
        Ok((16, 16))
    );
    let deep = format!("{}{}", "<g>".repeat(65), "</g>".repeat(65));
    assert!(validate_image("image/svg+xml", &svg(&deep)).is_err());
}

#[test]
fn preview_descriptor_schema_matches_rust_authority() {
    let bytes = serde_json::to_string_pretty(&descriptor_schema()).unwrap() + "\n";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/reference/hub-publish/v1/previews.schema.json");
    if std::env::var_os("GRAPHFORGE_UPDATE_PREVIEW_SCHEMA").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), bytes);
}

#[test]
fn invalid_zlib_adler_is_refused_even_when_idat_crc_is_valid() {
    let mut bytes = png(2, 3);
    let mut at = 8;
    loop {
        let size = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if &bytes[at + 4..at + 8] == b"IDAT" {
            bytes[at + 8 + size - 1] ^= 1; // Corrupt zlib Adler footer, not PNG framing.
            let mut crc = 0xffff_ffff_u32;
            for byte in &bytes[at + 4..at + 8 + size] {
                crc ^= u32::from(*byte);
                for _ in 0..8 {
                    crc = (crc >> 1) ^ if crc & 1 == 1 { 0xedb8_8320 } else { 0 };
                }
            }
            bytes[at + 8 + size..at + 12 + size].copy_from_slice(&(!crc).to_be_bytes());
            break;
        }
        at += size + 12;
    }
    assert_eq!(
        validate_image("image/png", &bytes),
        Err(PreviewError::Content)
    );
}

#[test]
fn corrupt_allowed_ancillary_crc_is_not_silently_skipped() {
    let bytes = png(1, 1);
    let mut bad = bytes[..33].to_vec();
    bad.extend_from_slice(&[0, 0, 0, 1, b's', b'R', b'G', b'B', 0, 0, 0, 0, 0]);
    bad.extend_from_slice(&bytes[33..]);
    assert_eq!(
        validate_image("image/png", &bad),
        Err(PreviewError::Content)
    );
}

#[test]
fn xml_preflight_bounds_dom_preallocation_and_per_element_attribute_work() {
    let pathological = svg(&format!("<path id=\"{}\"/>", "=".repeat(100_001)));
    assert_eq!(
        validate_image("image/svg+xml", &pathological),
        Err(PreviewError::Limit)
    );
    let mut attrs = String::new();
    for n in 0..65 {
        write!(attrs, " a{n}=\"v\"").unwrap();
    }
    assert_eq!(
        validate_image("image/svg+xml", &svg(&format!("<path{attrs}/>"))),
        Err(PreviewError::Limit)
    );
    // Tokenization respects quoted delimiters; this is one bounded attribute,
    // subsequently accepted by the closed vocabulary, not misread as attributes.
    assert_eq!(
        validate_image("image/svg+xml", &svg("<path d=\"M0 0=1\"/>")),
        Ok((16, 16))
    );
}
