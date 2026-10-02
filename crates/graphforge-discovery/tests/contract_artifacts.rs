//! Deterministic generator and parity test for public discovery artifacts.
#![allow(
    clippy::needless_pass_by_value,
    clippy::semicolon_if_nothing_returned,
    clippy::struct_field_names,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use graphforge_discovery::{
    DiscoveryErrorCode, DiscoveryLimits, DiscoveryManifest, ExactIdentity, ProjectSummary, RefSet,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

const MANIFEST_SCHEMA: &str =
    include_str!("../../../docs/reference/discovery/v1/manifest.schema.json");
const REFS_SCHEMA: &str = include_str!("../../../docs/reference/discovery/v1/refs.schema.json");
const SUMMARY_SCHEMA: &str =
    include_str!("../../../docs/reference/discovery/v1/summary.schema.json");
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
    Summary,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_summary_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_module_package_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_ontology_entries: Option<usize>,
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
            "summary":{"$ref":"#/$defs/summary_reference"}, "ontology":{"$ref":"#/$defs/ontology_inventory"},
            "extensions":{"$ref":"#/$defs/extensions"}
        }),
        {
            let mut defs = common_defs();
            let map = defs.as_object_mut().unwrap();
            map.insert("semantic".into(), json!({"type":"object","additionalProperties":false,"required":["capability","major"],"properties":{"capability":{"type":"string","minLength":1,"maxLength":128},"major":{"type":"integer","minimum":0,"maximum":65535}}}));
            map.insert("object".into(), json!({"type":"object","additionalProperties":false,"required":["digest","length","media_type","locations"],"properties":{"digest":{"$ref":"#/$defs/digest"},"length":{"type":"integer","minimum":0},"media_type":{"type":"string","minLength":1,"maxLength":4096},"locations":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"string","format":"uri","pattern":"^https://"}}}}));
            map.insert("package_reference".into(), json!({"type":"object","additionalProperties":false,"required":["format","package_digest","object_digest"],"properties":{"format":{"const":"graphforge-project/2"},"package_digest":{"$ref":"#/$defs/digest"},"object_digest":{"$ref":"#/$defs/digest"}}}));
            map.insert(
                "identity_text".into(),
                json!({"type":"string","minLength":1,"maxLength":4096}),
            );
            map.insert("summary_reference".into(), json!({"type":"object","additionalProperties":false,"required":["format","summary_digest","object_digest"],"properties":{"format":{"const":"graphforge-project-summary/1"},"summary_digest":{"$ref":"#/$defs/digest"},"object_digest":{"$ref":"#/$defs/digest"}}}));
            map.insert("module_descriptor".into(), json!({"type":"object","additionalProperties":false,"required":["id","version","content_digest"],"properties":{"id":{"$ref":"#/$defs/identity_text"},"version":{"$ref":"#/$defs/identity_text"},"content_digest":{"$ref":"#/$defs/digest"},"package":{"$ref":"#/$defs/package_reference"}}}));
            map.insert("bridge_descriptor".into(), bridge_descriptor_schema());
            map.insert("ontology_inventory".into(), json!({"type":"object","additionalProperties":false,"required":["composition_digest","modules","bridge_sets"],"properties":{"composition_digest":{"$ref":"#/$defs/digest"},"modules":{"type":"array","maxItems":4096,"items":{"$ref":"#/$defs/module_descriptor"}},"bridge_sets":{"type":"array","maxItems":4096,"items":{"$ref":"#/$defs/bridge_descriptor"}}}}));
            defs
        },
    )
}

fn bridge_descriptor_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"required":["id","version","content_digest"],"properties":{"id":{"$ref":"#/$defs/identity_text"},"version":{"$ref":"#/$defs/identity_text"},"content_digest":{"$ref":"#/$defs/digest"}}})
}

const COMPONENT_KINDS: [&str; 10] = [
    "ontology",
    "schema",
    "migration",
    "settings",
    "graph-data",
    "derived-artifact",
    "evidence",
    "provenance",
    "compatibility",
    "research",
];

fn summary_schema() -> Value {
    schema(
        "summary",
        &[
            "format",
            "version",
            "repository",
            "immutable_version",
            "package",
            "requirements",
            "capabilities",
            "metadata",
            "facts",
        ],
        json!({
            "format":{"const":"graphforge-project-summary/1"}, "version":{"$ref":"#/$defs/version"},
            "repository":{"$ref":"#/$defs/identity"}, "immutable_version":{"$ref":"#/$defs/digest"},
            "package":{"type":"object","additionalProperties":false,"required":["format","package_digest","package_class"],"properties":{"format":{"const":"graphforge-project/2"},"package_digest":{"$ref":"#/$defs/digest"},"package_class":{"enum":["complete","ontology-only","component-selective","graph-data-subset"]}}},
            "requirements":{"type":"array","maxItems":1,"items":{"type":"object","additionalProperties":false,"required":["capability","major"],"properties":{"capability":{"const":"project-summary"},"major":{"const":1}}}},
            "capabilities":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/semantic"}},
            "metadata":{"$ref":"#/$defs/metadata"}, "facts":{"$ref":"#/$defs/facts"},
            "extensions":{"$ref":"#/$defs/extensions"}
        }),
        {
            let mut defs = common_defs();
            let map = defs.as_object_mut().unwrap();
            map.insert("semantic".into(), json!({"type":"object","additionalProperties":false,"required":["capability","major"],"properties":{"capability":{"type":"string","minLength":1,"maxLength":128},"major":{"type":"integer","minimum":0,"maximum":65535}}}));
            map.insert("text".into(), json!({"type":"string","minLength":1,"maxLength":4096,"pattern":"^[^\\u0000-\\u001f\\u007f]+$"}));
            map.insert(
                "optional_text".into(),
                json!({"anyOf":[{"$ref":"#/$defs/text"},{"type":"null"}]}),
            );
            map.insert(
                "optional_count".into(),
                json!({"anyOf":[{"type":"integer","minimum":0},{"type":"null"}]}),
            );
            map.insert("text_list".into(), json!({"type":"array","maxItems":256,"uniqueItems":true,"items":{"$ref":"#/$defs/text"}}));
            map.insert(
                "identity_text".into(),
                json!({"type":"string","minLength":1,"maxLength":4096}),
            );
            map.insert("bridge_descriptor".into(), bridge_descriptor_schema());
            map.insert("metadata".into(), json!({"type":"object","additionalProperties":false,
                "required":["title","description","authors","subjects","languages","geographic_coverage","temporal_coverage","source_types","corpus_size","ontologies","license","access","tags","originating_projects","related_projects","canonical_identifiers","external_identifiers","created_at","updated_at"],
                "properties":{
                    "title":{"$ref":"#/$defs/optional_text"},"description":{"$ref":"#/$defs/optional_text"},
                    "authors":{"$ref":"#/$defs/text_list"},"subjects":{"$ref":"#/$defs/text_list"},"languages":{"$ref":"#/$defs/text_list"},
                    "geographic_coverage":{"anyOf":[{"type":"object","additionalProperties":false,"required":["label","regions"],"properties":{"label":{"$ref":"#/$defs/optional_text"},"regions":{"$ref":"#/$defs/text_list"}}},{"type":"null"}]},
                    "temporal_coverage":{"anyOf":[{"type":"object","additionalProperties":false,"required":["start","end","label"],"properties":{"start":{"$ref":"#/$defs/optional_text"},"end":{"$ref":"#/$defs/optional_text"},"label":{"$ref":"#/$defs/optional_text"}}},{"type":"null"}]},
                    "source_types":{"$ref":"#/$defs/text_list"},
                    "corpus_size":{"anyOf":[{"type":"object","additionalProperties":false,"required":["node_count","relationship_count","source_count","artifact_count"],"properties":{"node_count":{"$ref":"#/$defs/optional_count"},"relationship_count":{"$ref":"#/$defs/optional_count"},"source_count":{"$ref":"#/$defs/optional_count"},"artifact_count":{"$ref":"#/$defs/optional_count"}}},{"type":"null"}]},
                    "ontologies":{"$ref":"#/$defs/text_list"},"license":{"$ref":"#/$defs/optional_text"},
                    "access":{"type":"object","additionalProperties":false,"required":["visibility","access_policy"],"properties":{"visibility":{"$ref":"#/$defs/optional_text"},"access_policy":{"$ref":"#/$defs/optional_text"}}},
                    "tags":{"$ref":"#/$defs/text_list"},"originating_projects":{"$ref":"#/$defs/text_list"},"related_projects":{"$ref":"#/$defs/text_list"},
                    "canonical_identifiers":{"$ref":"#/$defs/text_list"},"external_identifiers":{"$ref":"#/$defs/text_list"},
                    "created_at":{"$ref":"#/$defs/optional_text"},"updated_at":{"$ref":"#/$defs/optional_text"}
                }}));
            map.insert("facts".into(), json!({"type":"object","additionalProperties":false,
                "required":["ontology_mode","components","research_present","evidence_present","payload_bytes","ontology_composition"],
                "properties":{
                    "ontology_mode":{"enum":["none","advisory","strict"]},
                    "components":{"type":"object","propertyNames":{"enum":COMPONENT_KINDS},"additionalProperties":{"type":"integer","minimum":1}},
                    "research_present":{"type":"boolean"},"evidence_present":{"type":"boolean"},
                    "payload_bytes":{"type":"integer","minimum":0},
                    "ontology_composition":{"anyOf":[{"type":"object","additionalProperties":false,"required":["composition_digest","modules","bridge_sets"],"properties":{
                        "composition_digest":{"$ref":"#/$defs/digest"},
                        "modules":{"type":"array","maxItems":4096,"items":{"type":"object","additionalProperties":false,"required":["id","version","content_digest","dialect","profile"],"properties":{"id":{"$ref":"#/$defs/identity_text"},"version":{"$ref":"#/$defs/identity_text"},"content_digest":{"$ref":"#/$defs/digest"},"dialect":{"$ref":"#/$defs/identity_text"},"profile":{"$ref":"#/$defs/identity_text"}}}},
                        "bridge_sets":{"type":"array","maxItems":4096,"items":{"$ref":"#/$defs/bridge_descriptor"}}}},{"type":"null"}]}
                }}));
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

const MODULE_ID: &str = "https://openalex.org/ontology/works";
const BRIDGE_ID: &str = "https://openalex.org/bridge/works-authors";

fn base_summary() -> Value {
    json!({
        "format":"graphforge-project-summary/1","version":{"major":1,"minor":1},
        "repository":{"owner":"openalex","repository":"openalex"},
        "immutable_version":digest('a'),
        "package":{"format":"graphforge-project/2","package_digest":digest('b'),"package_class":"complete"},
        "requirements":[{"capability":"project-summary","major":1}],"capabilities":[],
        "metadata":{
            "title":"OpenAlex","description":null,
            "authors":["OurResearch"],"subjects":["scholarly-communication"],"languages":["en"],
            "geographic_coverage":{"label":"Global","regions":["africa","asia"]},
            "temporal_coverage":{"start":"1900","end":null,"label":null},
            "source_types":["bibliographic-database"],
            "corpus_size":{"node_count":1000,"relationship_count":2500,"source_count":1,"artifact_count":null},
            "ontologies":[MODULE_ID],"license":"CC0-1.0",
            "access":{"visibility":"public","access_policy":"open"},
            "tags":[],"originating_projects":[],"related_projects":[],
            "canonical_identifiers":[],"external_identifiers":[],
            "created_at":"2026-01-01T00:00:00.000000Z","updated_at":"2026-01-01T00:00:00.000000Z"
        },
        "facts":{
            "ontology_mode":"advisory",
            "components":{"ontology":1,"research":1,"settings":2},
            "research_present":true,"evidence_present":false,"payload_bytes":4096,
            "ontology_composition":{
                "composition_digest":digest('e'),
                "modules":[{"id":MODULE_ID,"version":"2026.01","content_digest":digest('f'),"dialect":"graphforge-ontology","profile":"advisory"}],
                "bridge_sets":[{"id":BRIDGE_ID,"version":"1","content_digest":digest('9')}]
            }
        },
        "extensions":{"x-example":true}
    })
}

fn summary_digest(summary: &Value) -> String {
    ProjectSummary::from_json(compact(summary).as_bytes(), DiscoveryLimits::default())
        .unwrap()
        .canonical_digest()
        .unwrap()
        .0
}

/// Manifest advertising `summary` and an ontology inventory that matches it.
fn manifest_with_summary_and_ontology_for(summary: &Value) -> Value {
    let mut v = base_manifest();
    v["summary"] = json!({"format":"graphforge-project-summary/1","summary_digest":summary_digest(summary),"object_digest":digest('d')});
    v["ontology"] = json!({
        "composition_digest":digest('e'),
        "modules":[{"id":MODULE_ID,"version":"2026.01","content_digest":digest('f'),
            "package":{"format":"graphforge-project/2","package_digest":digest('1'),"object_digest":digest('2')}}],
        "bridge_sets":[{"id":BRIDGE_ID,"version":"1","content_digest":digest('9')}]
    });
    v["objects"] = json!([
        {"digest":digest('2'),"length":2048,"media_type":"application/vnd.graphforge.project","locations":["https://data.graphforge.sh/objects/2"]},
        {"digest":digest('c'),"length":42,"media_type":"application/vnd.graphforge.project","locations":["https://data.graphforge.sh/objects/c"]},
        {"digest":digest('d'),"length":900,"media_type":"application/vnd.graphforge.project-summary+json","locations":["https://data.graphforge.sh/objects/d"]}
    ]);
    v
}

fn manifest_with_summary_and_ontology() -> Value {
    manifest_with_summary_and_ontology_for(&base_summary())
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
        Document::Summary => String::from_utf8(
            ProjectSummary::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
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
    document: Document,
    value: Value,
    field: &str,
    subject: &str,
    supported_major: Option<u16>,
    requested_major: u16,
) -> Case {
    Case {
        name: name.into(),
        document,
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
        Document::Manifest,
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
        Document::Manifest,
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
        Document::Manifest,
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
            Document::Summary => base_summary(),
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
    cases.extend(summary_cases());
    Corpus {
        format: "graphforge-discovery-conformance/1".into(),
        cases,
    }
}

/// Cases for the Project summary document and the optional manifest fields.
/// Appended after the clone-oriented cases, which stay unchanged.
fn summary_cases() -> Vec<Case> {
    let mut cases = vec![
        valid(
            "manifest-with-summary-and-ontology",
            Document::Manifest,
            manifest_with_summary_and_ontology(),
        ),
        valid("summary-canonical", Document::Summary, base_summary()),
        // The unchanged clone manifest: `summary` and `ontology` are optional.
        valid(
            "manifest-without-summary-still-valid",
            Document::Manifest,
            base_manifest(),
        ),
        valid("summary-unknown-optional-capability", Document::Summary, {
            let mut v = base_summary();
            v["capabilities"] = json!([{"capability":"future-facts","major":3}]);
            v
        }),
        valid(
            "manifest-ontology-module-without-package",
            Document::Manifest,
            {
                let mut v = manifest_with_summary_and_ontology();
                v["ontology"]["modules"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("package");
                v["objects"].as_array_mut().unwrap().remove(0);
                v
            },
        ),
    ];
    cases.push(invalid_version(
        "summary-unknown-required-capability",
        Document::Summary,
        {
            let mut v = base_summary();
            v["requirements"] = json!([{"capability":"future-summary","major":1}]);
            v
        },
        "requirements",
        "capability",
        None,
        1,
    ));
    cases.push(invalid_version(
        "summary-future-required-capability-major",
        Document::Summary,
        {
            let mut v = base_summary();
            v["requirements"] = json!([{"capability":"project-summary","major":2}]);
            v
        },
        "requirements",
        "capability",
        Some(1),
        2,
    ));
    cases.push(invalid_version(
        "summary-future-format",
        Document::Summary,
        {
            let mut v = base_summary();
            v["format"] = json!("graphforge-project-summary/2");
            v
        },
        "format",
        "project_summary",
        Some(1),
        2,
    ));
    cases.push(invalid_version(
        "summary-future-package",
        Document::Summary,
        {
            let mut v = base_summary();
            v["package"]["format"] = json!("graphforge-project/3");
            v
        },
        "package.format",
        "portable_package",
        Some(2),
        3,
    ));
    cases.push(invalid_version(
        "manifest-future-summary-format",
        Document::Manifest,
        {
            let mut v = manifest_with_summary_and_ontology();
            v["summary"]["format"] = json!("graphforge-project-summary/2");
            v
        },
        "summary.format",
        "project_summary",
        Some(1),
        2,
    ));
    cases.push(invalid_version(
        "manifest-module-package-future-format",
        Document::Manifest,
        {
            let mut v = manifest_with_summary_and_ontology();
            v["ontology"]["modules"][0]["package"]["format"] = json!("graphforge-project/3");
            v
        },
        "ontology.modules.package.format",
        "portable_package",
        Some(2),
        3,
    ));
    let mutations: &[(&str, Document, &str, Option<&str>, fn(&mut Value))] = &[
        (
            "manifest-missing-summary-object",
            Document::Manifest,
            "missing_object",
            Some("summary.object_digest"),
            |v| v["summary"]["object_digest"] = json!(digest('7')),
        ),
        (
            "manifest-summary-object-media-type",
            Document::Manifest,
            "malformed_response",
            Some("summary.object_digest"),
            |v| v["objects"][2]["media_type"] = json!("application/octet-stream"),
        ),
        (
            "manifest-summary-is-project-package",
            Document::Manifest,
            "malformed_response",
            Some("summary.object_digest"),
            |v| v["summary"]["object_digest"] = json!(digest('c')),
        ),
        (
            "manifest-summary-unknown-field",
            Document::Manifest,
            "malformed_response",
            None,
            |v| v["summary"]["url"] = json!("https://data.graphforge.sh/summary"),
        ),
        (
            "manifest-module-package-missing-object",
            Document::Manifest,
            "missing_object",
            Some("ontology.modules.package.object_digest"),
            |v| v["ontology"]["modules"][0]["package"]["object_digest"] = json!(digest('7')),
        ),
        (
            "manifest-module-package-is-project-package",
            Document::Manifest,
            "malformed_response",
            Some("ontology.modules.package.object_digest"),
            |v| v["ontology"]["modules"][0]["package"]["object_digest"] = json!(digest('c')),
        ),
        (
            "manifest-module-package-media-type",
            Document::Manifest,
            "malformed_response",
            Some("ontology.modules.package.object_digest"),
            |v| v["objects"][0]["media_type"] = json!("application/octet-stream"),
        ),
        (
            "manifest-noncanonical-ontology-modules",
            Document::Manifest,
            "duplicate",
            Some("ontology.modules"),
            |v| {
                let module = v["ontology"]["modules"][0].clone();
                v["ontology"]["modules"]
                    .as_array_mut()
                    .unwrap()
                    .push(module);
            },
        ),
        (
            "manifest-noncanonical-ontology-bridge-sets",
            Document::Manifest,
            "duplicate",
            Some("ontology.bridge_sets"),
            |v| {
                let bridge = v["ontology"]["bridge_sets"][0].clone();
                v["ontology"]["bridge_sets"]
                    .as_array_mut()
                    .unwrap()
                    .push(bridge);
            },
        ),
        (
            "manifest-ontology-invalid-digest",
            Document::Manifest,
            "integrity_failure",
            Some("digest"),
            |v| v["ontology"]["composition_digest"] = json!("sha256:ABC"),
        ),
        (
            "summary-unknown-field",
            Document::Summary,
            "malformed_response",
            None,
            |v| v["surprise"] = json!(true),
        ),
        (
            "summary-rejects-collaborators",
            Document::Summary,
            "malformed_response",
            None,
            |v| v["metadata"]["access"]["collaborators"] = json!(["private-person"]),
        ),
        (
            "summary-rejects-metadata-extensions",
            Document::Summary,
            "malformed_response",
            None,
            |v| v["metadata"]["extensions"] = json!({"internal": "value"}),
        ),
        (
            "summary-inconsistent-research-presence",
            Document::Summary,
            "malformed_response",
            Some("facts"),
            |v| v["facts"]["research_present"] = json!(false),
        ),
        (
            "summary-unknown-component-kind",
            Document::Summary,
            "malformed_response",
            Some("facts.components"),
            |v| v["facts"]["components"]["mystery"] = json!(1),
        ),
        (
            "summary-zero-component-count",
            Document::Summary,
            "malformed_response",
            Some("facts.components"),
            |v| v["facts"]["components"]["schema"] = json!(0),
        ),
        (
            "summary-unknown-ontology-mode",
            Document::Summary,
            "malformed_response",
            Some("facts.ontology_mode"),
            |v| v["facts"]["ontology_mode"] = json!("lenient"),
        ),
        (
            "summary-unknown-package-class",
            Document::Summary,
            "malformed_response",
            Some("package.package_class"),
            |v| v["package"]["package_class"] = json!("everything"),
        ),
        (
            "summary-noncanonical-modules",
            Document::Summary,
            "duplicate",
            Some("facts.ontology_composition.modules"),
            |v| {
                let module = v["facts"]["ontology_composition"]["modules"][0].clone();
                v["facts"]["ontology_composition"]["modules"]
                    .as_array_mut()
                    .unwrap()
                    .push(module);
            },
        ),
        (
            "summary-metadata-unsorted-list",
            Document::Summary,
            "duplicate",
            Some("metadata.authors"),
            |v| v["metadata"]["authors"] = json!(["b", "a"]),
        ),
        (
            "summary-metadata-control-character",
            Document::Summary,
            "malformed_response",
            Some("metadata.title"),
            |v| v["metadata"]["title"] = json!("bad\u{7}title"),
        ),
        (
            "summary-metadata-empty-string",
            Document::Summary,
            "limit_exceeded",
            Some("metadata.license"),
            |v| v["metadata"]["license"] = json!(""),
        ),
        (
            "summary-noncanonical-requirements",
            Document::Summary,
            "duplicate",
            Some("requirements"),
            |v| {
                v["requirements"] = json!([
                    {"capability":"project-summary","major":1},
                    {"capability":"project-summary","major":1}
                ])
            },
        ),
    ];
    for (name, doc, code, field, mutate) in mutations {
        let mut value = match doc {
            Document::Manifest => manifest_with_summary_and_ontology(),
            Document::Refs => base_refs(),
            Document::Summary => base_summary(),
        };
        mutate(&mut value);
        cases.push(invalid(name, *doc, value, code, *field));
    }
    let bounded = |name: &str, document: Document, value: Value, field: &str, limits: Limits| {
        let mut case = invalid(name, document, value, "limit_exceeded", Some(field));
        case.limits = Some(limits);
        case
    };
    cases.push(bounded(
        "summary-byte-bound",
        Document::Summary,
        base_summary(),
        "response",
        Limits {
            max_response_bytes: Some(1),
            ..Limits::default()
        },
    ));
    cases.push(bounded(
        "summary-document-length-bound",
        Document::Summary,
        base_summary(),
        "summary",
        Limits {
            max_summary_bytes: Some(1),
            ..Limits::default()
        },
    ));
    cases.push(bounded(
        "manifest-summary-length-bound",
        Document::Manifest,
        manifest_with_summary_and_ontology(),
        "summary.object_digest",
        Limits {
            max_summary_bytes: Some(899),
            ..Limits::default()
        },
    ));
    cases.push(bounded(
        "manifest-module-package-length-bound",
        Document::Manifest,
        manifest_with_summary_and_ontology(),
        "ontology.modules.package.object_digest",
        Limits {
            max_module_package_bytes: Some(2047),
            ..Limits::default()
        },
    ));
    cases.push(bounded(
        "manifest-ontology-module-count-bound",
        Document::Manifest,
        manifest_with_summary_and_ontology(),
        "ontology.modules",
        Limits {
            max_ontology_entries: Some(0),
            ..Limits::default()
        },
    ));
    cases.push(bounded(
        "summary-ontology-module-count-bound",
        Document::Summary,
        base_summary(),
        "facts.ontology_composition.modules",
        Limits {
            max_ontology_entries: Some(0),
            ..Limits::default()
        },
    ));
    cases
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
        if let Some(x) = v.max_summary_bytes {
            limits.max_summary_bytes = x
        }
        if let Some(x) = v.max_module_package_bytes {
            limits.max_module_package_bytes = x
        }
        if let Some(x) = v.max_ontology_entries {
            limits.max_ontology_entries = x
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
        ("summary.schema.json", pretty(&summary_schema())),
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
    for ((name, expected), actual) in
        expected
            .iter()
            .zip([MANIFEST_SCHEMA, REFS_SCHEMA, SUMMARY_SCHEMA, FIXTURES])
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
            Document::Summary => {
                ProjectSummary::from_json(case.json.as_bytes(), limits(case.limits))
                    .map(|v| String::from_utf8(v.to_canonical_json().unwrap()).unwrap())
            }
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

fn parse_summary(value: &Value) -> ProjectSummary {
    ProjectSummary::from_json(compact(value).as_bytes(), DiscoveryLimits::default()).unwrap()
}

fn parse_manifest(value: &Value) -> DiscoveryManifest {
    DiscoveryManifest::from_json(compact(value).as_bytes(), DiscoveryLimits::default()).unwrap()
}

fn bind_error(
    manifest: &Value,
    summary: &ProjectSummary,
) -> (DiscoveryErrorCode, Option<&'static str>) {
    let error = parse_manifest(manifest).bind_summary(summary).unwrap_err();
    (error.code, error.field)
}

#[test]
fn bind_summary_accepts_the_advertised_summary() {
    let summary = parse_summary(&base_summary());
    parse_manifest(&manifest_with_summary_and_ontology())
        .bind_summary(&summary)
        .unwrap();
    // A Project without ontology composition binds with no `ontology` field.
    let mut plain = base_summary();
    plain["facts"]["ontology_composition"] = Value::Null;
    let mut manifest = manifest_with_summary_and_ontology_for(&plain);
    manifest.as_object_mut().unwrap().remove("ontology");
    manifest["objects"].as_array_mut().unwrap().remove(0);
    parse_manifest(&manifest)
        .bind_summary(&parse_summary(&plain))
        .unwrap();
}

#[test]
fn bind_summary_requires_an_advertised_summary() {
    let summary = parse_summary(&base_summary());
    let error = parse_manifest(&base_manifest())
        .bind_summary(&summary)
        .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::MissingObject);
    assert_eq!(error.field, Some("summary"));
    assert!(parse_manifest(&base_manifest()).summary_object().is_err());
}

#[test]
fn bind_summary_rejects_each_identity_mismatch() {
    let manifest = manifest_with_summary_and_ontology();
    let integrity = DiscoveryErrorCode::IntegrityFailure;

    let mut other = base_summary();
    other["repository"]["repository"] = json!("elsewhere");
    let manifest_for_other = manifest_with_summary_and_ontology_for(&other);
    assert_eq!(
        bind_error(&manifest_for_other, &parse_summary(&other)),
        (integrity, Some("summary.repository"))
    );

    let mut other = base_summary();
    other["immutable_version"] = json!(digest('7'));
    assert_eq!(
        bind_error(
            &manifest_with_summary_and_ontology_for(&other),
            &parse_summary(&other)
        ),
        (integrity, Some("summary.immutable_version"))
    );

    let mut other = base_summary();
    other["package"]["package_digest"] = json!(digest('7'));
    assert_eq!(
        bind_error(
            &manifest_with_summary_and_ontology_for(&other),
            &parse_summary(&other)
        ),
        (integrity, Some("summary.package.package_digest"))
    );

    // Same identity fields, different bytes: the manifest's summary_digest no
    // longer matches.
    let mut other = base_summary();
    other["metadata"]["title"] = json!("Different title");
    assert_eq!(
        bind_error(&manifest, &parse_summary(&other)),
        (integrity, Some("summary.summary_digest"))
    );
}

#[test]
fn bind_summary_rejects_ontology_inventory_disagreement() {
    let integrity = DiscoveryErrorCode::IntegrityFailure;
    let mutations: &[fn(&mut Value)] = &[
        |v| v["facts"]["ontology_composition"]["composition_digest"] = json!(digest('7')),
        |v| v["facts"]["ontology_composition"]["modules"][0]["content_digest"] = json!(digest('7')),
        |v| v["facts"]["ontology_composition"]["modules"][0]["version"] = json!("2026.02"),
        |v| v["facts"]["ontology_composition"]["bridge_sets"] = json!([]),
        |v| v["facts"]["ontology_composition"] = Value::Null,
    ];
    for mutate in mutations {
        let mut other = base_summary();
        mutate(&mut other);
        // The manifest references the mutated summary, so only the inventory
        // comparison can fail.
        assert_eq!(
            bind_error(
                &manifest_with_summary_and_ontology_for(&other),
                &parse_summary(&other)
            ),
            (integrity, Some("summary"))
        );
    }
    // A manifest that omits its inventory disagrees with a summary that has one.
    let mut manifest = manifest_with_summary_and_ontology();
    manifest.as_object_mut().unwrap().remove("ontology");
    manifest["objects"].as_array_mut().unwrap().remove(0);
    assert_eq!(
        bind_error(&manifest, &parse_summary(&base_summary())),
        (integrity, Some("summary"))
    );
}

#[test]
fn summary_and_ontology_descriptors_do_not_depend_on_object_locations() {
    let summary = parse_summary(&base_summary());
    let first = manifest_with_summary_and_ontology();
    let mut second = manifest_with_summary_and_ontology();
    for object in second["objects"].as_array_mut().unwrap() {
        let digest = object["digest"].as_str().unwrap().replace("sha256:", "");
        object["locations"] = json!([
            format!("https://cdn.example.org/mirror/{digest}"),
            format!("https://data.graphforge.sh/other/{digest}"),
        ]);
    }
    let (first, second) = (parse_manifest(&first), parse_manifest(&second));
    first.bind_summary(&summary).unwrap();
    second.bind_summary(&summary).unwrap();

    // Only transport changed: the manifest identity differs, nothing else does.
    assert_ne!(
        first.canonical_digest().unwrap(),
        second.canonical_digest().unwrap()
    );
    assert_eq!(first.summary, second.summary);
    assert_eq!(first.ontology, second.ontology);
    assert_eq!(
        first.summary.as_ref().unwrap().summary_digest,
        summary.canonical_digest().unwrap()
    );
    let text = String::from_utf8(summary.to_canonical_json().unwrap()).unwrap();
    for object in first.objects.iter().chain(&second.objects) {
        for location in &object.locations {
            assert!(
                !text.contains(location.as_str()),
                "a summary carries no object location"
            );
        }
    }
}

#[test]
fn exact_module_objects_are_selected_by_identity_and_never_by_position() {
    let manifest = parse_manifest(&manifest_with_summary_and_ontology());
    let identity = ExactIdentity {
        id: MODULE_ID.into(),
        version: "2026.01".into(),
        content_digest: graphforge_discovery::Sha256Digest(digest('f')),
    };
    let (descriptor, object) = manifest.ontology_module_object(&identity).unwrap();
    assert_eq!(descriptor.identity(), identity);
    assert_eq!(object.digest.0, digest('2'));
    assert_ne!(object.digest, manifest.package.object_digest);
    assert_eq!(manifest.summary_object().unwrap().digest.0, digest('d'));

    let mut unknown = identity.clone();
    unknown.content_digest = graphforge_discovery::Sha256Digest(digest('7'));
    let error = manifest.ontology_module_object(&unknown).unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::MissingObject);
    assert_eq!(error.field, Some("ontology.modules"));

    let mut without_package = manifest_with_summary_and_ontology();
    without_package["ontology"]["modules"][0]
        .as_object_mut()
        .unwrap()
        .remove("package");
    without_package["objects"].as_array_mut().unwrap().remove(0);
    let error = parse_manifest(&without_package)
        .ontology_module_object(&identity)
        .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::MissingObject);
    assert_eq!(error.field, Some("ontology.modules"));
}

#[test]
fn summary_string_bounds_follow_project_metadata_bounds() {
    let mut value = base_summary();
    value["metadata"]["title"] = json!("a".repeat(4097));
    let error = ProjectSummary::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
        .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::LimitExceeded);
    assert_eq!(error.field, Some("metadata.title"));
    value["metadata"]["title"] = json!("a".repeat(4096));
    parse_summary(&value);

    let mut value = base_summary();
    value["metadata"]["tags"] = json!((0..257).map(|i| format!("tag-{i:04}")).collect::<Vec<_>>());
    let error = ProjectSummary::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
        .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::LimitExceeded);
    assert_eq!(error.field, Some("metadata.tags"));
}

#[test]
fn unknown_required_summary_capability_fails_before_metadata_is_considered() {
    // The metadata below is invalid, but the unsupported requirement is
    // reported first, so a reader never interprets content it cannot honor.
    let mut value = base_summary();
    value["requirements"] = json!([{"capability":"future-summary","major":1}]);
    value["metadata"]["authors"] = json!(["b", "a"]);
    let error = ProjectSummary::from_json(compact(&value).as_bytes(), DiscoveryLimits::default())
        .unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::UnsupportedFuture);
    assert_eq!(error.field, Some("requirements"));
}
