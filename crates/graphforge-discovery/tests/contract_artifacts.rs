//! Deterministic generator and parity test for public discovery artifacts.
#![allow(
    clippy::needless_pass_by_value,
    clippy::semicolon_if_nothing_returned,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use graphforge_discovery::{DiscoveryLimits, DiscoveryManifest, RefSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

const MANIFEST_SCHEMA: &str =
    include_str!("../../../docs/reference/discovery/v1/manifest.schema.json");
const REFS_SCHEMA: &str = include_str!("../../../docs/reference/discovery/v1/refs.schema.json");
const FIXTURES: &str = include_str!("../../../docs/reference/discovery/v1/conformance.json");

#[derive(Debug, Deserialize, Serialize)]
struct Corpus {
    format: String,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Case {
    name: String,
    document: Document,
    json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limits: Option<Limits>,
    expected: Expected,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Document {
    Manifest,
    Refs,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_response_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_refs: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_objects: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_locations_per_object: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_cumulative_object_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum Expected {
    Valid {
        canonical_json: String,
    },
    Invalid {
        code: String,
        field: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<Value>,
    },
}

fn schema(title: &str, required: &[&str], properties: Value, defs: Value) -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": format!("https://graphforge.sh/schemas/discovery/v1/{title}.schema.json"),
        "title": title,
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties,
        "$defs": defs
    })
}

fn common_defs() -> Value {
    json!({
        "digest": {"type":"string","pattern":"^sha256:[0-9a-f]{64}$"},
        "identity": {"type":"object","additionalProperties":false,"required":["owner","repository"],"properties":{
            "owner":{"$ref":"#/$defs/slug"},"repository":{"$ref":"#/$defs/slug"}}},
        "slug": {"type":"string","minLength":1,"maxLength":100,"pattern":"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"},
        "version": {"type":"object","additionalProperties":false,"required":["major","minor"],"properties":{
            "major":{"type":"integer","minimum":0,"maximum":65535},"minor":{"type":"integer","minimum":0,"maximum":65535}}},
        "extensions": {"type":"object","maxProperties":256,"propertyNames":{"pattern":"^x-[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"}}
    })
}

fn manifest_schema() -> Value {
    schema(
        "manifest",
        &[
            "format",
            "version",
            "repository",
            "default_ref",
            "resolved_ref",
            "immutable_version",
            "package",
            "requirements",
            "capabilities",
            "objects",
        ],
        json!({
            "format":{"const":"graphforge-discovery/1"}, "version":{"$ref":"#/$defs/version"},
            "repository":{"$ref":"#/$defs/identity"}, "default_ref":{"type":"string","minLength":1,"maxLength":4096},
            "resolved_ref":{"type":"string","minLength":1,"maxLength":4096}, "immutable_version":{"$ref":"#/$defs/digest"},
            "package":{"type":"object","additionalProperties":false,"required":["format","package_digest","object_digest"],"properties":{"format":{"const":"graphforge-project/2"},"package_digest":{"$ref":"#/$defs/digest"},"object_digest":{"$ref":"#/$defs/digest"}}},
            "requirements":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/semantic"}},
            "capabilities":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/semantic"}},
            "objects":{"type":"array","minItems":1,"maxItems":1_000_000,"items":{"$ref":"#/$defs/object"}},
            "extensions":{"$ref":"#/$defs/extensions"}
        }),
        {
            let mut defs = common_defs();
            let map = defs.as_object_mut().unwrap();
            map.insert("semantic".into(), json!({"type":"object","additionalProperties":false,"required":["capability","major"],"properties":{"capability":{"type":"string","minLength":1,"maxLength":128},"major":{"type":"integer","minimum":0,"maximum":65535}}}));
            map.insert("object".into(), json!({"type":"object","additionalProperties":false,"required":["digest","length","media_type","locations"],"properties":{"digest":{"$ref":"#/$defs/digest"},"length":{"type":"integer","minimum":0},"media_type":{"type":"string","minLength":1,"maxLength":4096},"locations":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string","format":"uri","pattern":"^https://"}}}}));
            defs
        },
    )
}

fn refs_schema() -> Value {
    schema(
        "refs",
        &["format", "version", "repository", "default_ref", "refs"],
        json!({
            "format":{"const":"graphforge-discovery/1"}, "version":{"$ref":"#/$defs/version"},
            "repository":{"$ref":"#/$defs/identity"}, "default_ref":{"type":"string","minLength":1,"maxLength":4096},
            "refs":{"type":"array","maxItems":10000,"items":{"$ref":"#/$defs/ref"}}, "extensions":{"$ref":"#/$defs/extensions"}
        }),
        {
            let mut defs = common_defs();
            defs.as_object_mut().unwrap().insert("ref".into(), json!({"type":"object","additionalProperties":false,"required":["name","target","validator"],"properties":{"name":{"type":"string","minLength":1,"maxLength":4096},"target":{"$ref":"#/$defs/digest"},"validator":{"$ref":"#/$defs/digest"}}}));
            defs
        },
    )
}

fn digest(c: char) -> String {
    format!("sha256:{}", c.to_string().repeat(64))
}

fn base_manifest() -> Value {
    json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":0},"repository":{"owner":"openalex","repository":"openalex"},
        "default_ref":"main","resolved_ref":"main","immutable_version":digest('a'),
        "package":{"format":"graphforge-project/2","package_digest":digest('b'),"object_digest":digest('c')},
        "requirements":[{"capability":"portable-v2","major":1}],"capabilities":[{"capability":"range-requests","major":1}],
        "objects":[{"digest":digest('c'),"length":42,"media_type":"application/vnd.graphforge.project","locations":["https://data.graphforge.sh/objects/c"]}],
        "extensions":{"x-example":{"z":1,"a":true}}
    })
}

fn base_refs() -> Value {
    json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":0},"repository":{"owner":"openalex","repository":"openalex"},
        "default_ref":"main","refs":[{"name":"main","target":digest('a'),"validator":digest('d')}]
    })
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap()
}

fn valid(name: &str, document: Document, value: Value) -> Case {
    let canonical_json = match document {
        Document::Manifest => String::from_utf8(
            DiscoveryManifest::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
                .unwrap()
                .to_canonical_json()
                .unwrap(),
        )
        .unwrap(),
        Document::Refs => String::from_utf8(
            RefSet::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
                .unwrap()
                .to_canonical_json()
                .unwrap(),
        )
        .unwrap(),
    };
    Case {
        name: name.into(),
        document,
        json: compact(&value),
        limits: None,
        expected: Expected::Valid { canonical_json },
    }
}

fn invalid(name: &str, document: Document, value: Value, code: &str, field: Option<&str>) -> Case {
    Case {
        name: name.into(),
        document,
        json: compact(&value),
        limits: None,
        expected: Expected::Invalid {
            code: code.into(),
            field: field.map(str::to_owned),
            version: None,
        },
    }
}

fn invalid_version(
    name: &str,
    value: Value,
    field: &str,
    subject: &str,
    supported_major: Option<u16>,
    requested_major: u16,
) -> Case {
    Case {
        name: name.into(),
        document: Document::Manifest,
        json: compact(&value),
        limits: None,
        expected: Expected::Invalid {
            code: "unsupported_future".into(),
            field: Some(field.into()),
            version: Some(
                json!({"subject":subject,"supported_major":supported_major,"requested_major":requested_major}),
            ),
        },
    }
}

fn corpus() -> Corpus {
    let mut cases = vec![
        valid(
            "manifest-minor-compatible-and-canonical",
            Document::Manifest,
            {
                let mut v = base_manifest();
                v["version"]["minor"] = json!(99);
                v
            },
        ),
        valid("refs-canonical", Document::Refs, base_refs()),
    ];
    cases.push(invalid_version(
        "future-protocol",
        {
            let mut v = base_manifest();
            v["version"]["major"] = json!(2);
            v
        },
        "version.major",
        "protocol",
        Some(1),
        2,
    ));
    cases.push(invalid_version(
        "future-package",
        {
            let mut v = base_manifest();
            v["package"]["format"] = json!("graphforge-project/3");
            v
        },
        "package.format",
        "portable_package",
        Some(2),
        3,
    ));
    cases.push(invalid_version(
        "unknown-required-capability",
        {
            let mut v = base_manifest();
            v["requirements"][0]["capability"] = json!("future-fetch");
            v
        },
        "requirements",
        "capability",
        None,
        1,
    ));
    cases.push(valid("unknown-optional-capability", Document::Manifest, {
        let mut v = base_manifest();
        v["capabilities"][0]["capability"] = json!("future-fetch");
        v
    }));
    cases.push(valid("multiple-objects-select-explicit-package", Document::Manifest, {
        let mut v = base_manifest();
        v["objects"] = json!([
            {"digest":digest('a'),"length":7,"media_type":"application/octet-stream","locations":["https://data.graphforge.sh/objects/a"]},
            {"digest":digest('c'),"length":42,"media_type":"application/vnd.graphforge.project","locations":["https://data.graphforge.sh/objects/c"]}
        ]);
        v
    }));
    let mutations: &[(&str, Document, &str, Option<&str>, fn(&mut Value))] = &[
        (
            "older-package-reference-without-object",
            Document::Manifest,
            "malformed_response",
            None,
            |v| {
                v["package"]
                    .as_object_mut()
                    .unwrap()
                    .remove("object_digest");
            },
        ),
        (
            "missing-package-object",
            Document::Manifest,
            "missing_object",
            Some("package.object_digest"),
            |v| v["package"]["object_digest"] = json!(digest('d')),
        ),
        (
            "incompatible-package-object-media-type",
            Document::Manifest,
            "malformed_response",
            Some("package.object_digest"),
            |v| v["objects"][0]["media_type"] = json!("application/octet-stream"),
        ),
        (
            "invalid-identity",
            Document::Manifest,
            "invalid_identity",
            Some("repository"),
            |v| v["repository"]["owner"] = json!("OpenAlex"),
        ),
        (
            "invalid-digest",
            Document::Manifest,
            "integrity_failure",
            Some("digest"),
            |v| v["objects"][0]["digest"] = json!("sha256:ABC"),
        ),
        (
            "unsafe-http-url",
            Document::Manifest,
            "unsafe_location",
            Some("objects.locations"),
            |v| v["objects"][0]["locations"][0] = json!("http://data.graphforge.sh/object"),
        ),
        (
            "credentialed-url",
            Document::Manifest,
            "unsafe_location",
            Some("objects.locations"),
            |v| {
                v["objects"][0]["locations"][0] =
                    json!("https://user:secret@data.graphforge.sh/object")
            },
        ),
        (
            "query-url",
            Document::Manifest,
            "unsafe_location",
            Some("objects.locations"),
            |v| {
                v["objects"][0]["locations"][0] =
                    json!("https://data.graphforge.sh/object?token=secret")
            },
        ),
        (
            "duplicate-object",
            Document::Manifest,
            "duplicate",
            Some("objects.digest"),
            |v| {
                let x = v["objects"][0].clone();
                v["objects"].as_array_mut().unwrap().push(x)
            },
        ),
        (
            "missing-object",
            Document::Manifest,
            "missing_object",
            Some("objects"),
            |v| v["objects"] = json!([]),
        ),
        (
            "noncanonical-requirements",
            Document::Manifest,
            "duplicate",
            Some("requirements"),
            |v| v["requirements"] = json!([{"capability":"portable-v2","major":1},{"capability":"portable-v2","major":1}]),
        ),
        (
            "invalid-ref-name",
            Document::Refs,
            "malformed_response",
            Some("ref"),
            |v| v["refs"][0]["name"] = json!("bad..ref"),
        ),
        (
            "missing-default-ref",
            Document::Refs,
            "missing_ref",
            Some("default_ref"),
            |v| v["default_ref"] = json!("trunk"),
        ),
        (
            "invalid-validator",
            Document::Refs,
            "integrity_failure",
            Some("digest"),
            |v| v["refs"][0]["validator"] = json!("W/\"weak\""),
        ),
        (
            "duplicate-ref",
            Document::Refs,
            "duplicate",
            Some("refs.name"),
            |v| {
                let x = v["refs"][0].clone();
                v["refs"].as_array_mut().unwrap().push(x)
            },
        ),
    ];
    for (name, doc, code, field, mutate) in mutations {
        let mut value = match doc {
            Document::Manifest => base_manifest(),
            Document::Refs => base_refs(),
        };
        mutate(&mut value);
        cases.push(invalid(name, *doc, value, code, *field));
    }
    let mut bounded = invalid(
        "response-byte-bound",
        Document::Manifest,
        base_manifest(),
        "limit_exceeded",
        Some("response"),
    );
    bounded.limits = Some(Limits {
        max_response_bytes: Some(1),
        ..Limits::default()
    });
    cases.push(bounded);
    let mut bounded = invalid(
        "location-count-bound",
        Document::Manifest,
        {
            let mut v = base_manifest();
            v["objects"][0]["locations"] = json!(["https://a.example/o", "https://b.example/o"]);
            v
        },
        "limit_exceeded",
        Some("objects.locations"),
    );
    bounded.limits = Some(Limits {
        max_locations_per_object: Some(1),
        ..Limits::default()
    });
    cases.push(bounded);
    let mut bounded = invalid(
        "cumulative-byte-bound",
        Document::Manifest,
        base_manifest(),
        "limit_exceeded",
        Some("objects.length"),
    );
    bounded.limits = Some(Limits {
        max_cumulative_object_bytes: Some(41),
        ..Limits::default()
    });
    cases.push(bounded);
    let mut bounded = invalid(
        "ref-count-bound",
        Document::Refs,
        base_refs(),
        "limit_exceeded",
        Some("refs"),
    );
    bounded.limits = Some(Limits {
        max_refs: Some(0),
        ..Limits::default()
    });
    cases.push(bounded);
    cases.push(Case {
        name: "duplicate-json-member".into(),
        document: Document::Refs,
        json: "{\"format\":\"graphforge-discovery/1\",\"format\":\"graphforge-discovery/1\"}"
            .into(),
        limits: None,
        expected: Expected::Invalid {
            code: "malformed_response".into(),
            field: None,
            version: None,
        },
    });
    Corpus {
        format: "graphforge-discovery-conformance/1".into(),
        cases,
    }
}

fn limits(overrides: Option<Limits>) -> DiscoveryLimits {
    let mut limits = DiscoveryLimits::default();
    if let Some(v) = overrides {
        if let Some(x) = v.max_response_bytes {
            limits.max_response_bytes = x
        }
        if let Some(x) = v.max_refs {
            limits.max_refs = x
        }
        if let Some(x) = v.max_objects {
            limits.max_objects = x
        }
        if let Some(x) = v.max_locations_per_object {
            limits.max_locations_per_object = x
        }
        if let Some(x) = v.max_cumulative_object_bytes {
            limits.max_cumulative_object_bytes = x
        }
    }
    limits
}
fn pretty(value: &impl Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

#[test]
fn checked_in_contract_artifacts_match_rust_authority() {
    let expected = [
        ("manifest.schema.json", pretty(&manifest_schema())),
        ("refs.schema.json", pretty(&refs_schema())),
        ("conformance.json", pretty(&corpus())),
    ];
    if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
        let dir = root().join("docs/reference/discovery/v1");
        fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in &expected {
            fs::write(dir.join(name), bytes).unwrap();
        }
        return;
    }
    for ((name, expected), actual) in expected
        .iter()
        .zip([MANIFEST_SCHEMA, REFS_SCHEMA, FIXTURES])
    {
        assert_eq!(
            actual.as_bytes(),
            expected,
            "{name} is stale; regenerate with GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS=1 cargo test -p graphforge-discovery --test contract_artifacts"
        );
    }
    let parsed: Corpus = serde_json::from_str(FIXTURES).unwrap();
    for case in parsed.cases {
        let result = match case.document {
            Document::Manifest => {
                DiscoveryManifest::from_json(case.json.as_bytes(), limits(case.limits))
                    .map(|v| String::from_utf8(v.to_canonical_json().unwrap()).unwrap())
            }
            Document::Refs => RefSet::from_json(case.json.as_bytes(), limits(case.limits))
                .map(|v| String::from_utf8(v.to_canonical_json().unwrap()).unwrap()),
        };
        match (case.expected, result) {
            (Expected::Valid { canonical_json }, Ok(actual)) => {
                assert_eq!(actual, canonical_json, "{}", case.name)
            }
            (
                Expected::Invalid {
                    code,
                    field,
                    version,
                },
                Err(error),
            ) => {
                assert_eq!(
                    serde_json::to_value(error.code).unwrap(),
                    Value::String(code),
                    "{}",
                    case.name
                );
                assert_eq!(error.field.map(str::to_owned), field, "{}", case.name);
                assert_eq!(
                    serde_json::to_value(error.version).unwrap(),
                    version.unwrap_or(Value::Null),
                    "{}",
                    case.name
                )
            }
            (_, result) => panic!("{}: unexpected {result:?}", case.name),
        }
    }
}
