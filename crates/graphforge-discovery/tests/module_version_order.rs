//! Native generation and replay of opaque-label declared precedence cases.
use graphforge_discovery::{
    DeclaredModuleVersion, DiscoveryLimits, compare_module_versions, compare_module_versions_json,
};
use serde_json::{Value, json};
use std::path::PathBuf;

fn declaration(version: &str, marker: char, order: Option<u32>) -> Value {
    let mut value = json!({"module":{
        "id":"https://graphforge.sh/ont_aaaaaaaaaaaaaaaa", "version":version,
        "content_digest":format!("sha256:{}", marker.to_string().repeat(64))}});
    if let Some(order) = order {
        value["version_order"] = json!(order);
    }
    value
}
fn case(name: &str, a: &Value, b: &Value, expected: Value) -> Value {
    raw_case(name, &serde_json::to_string(a).unwrap(), b, expected)
}
fn raw_case(name: &str, a: &str, b: &Value, expected: Value) -> Value {
    json!({"name":name,"a":a,"b":serde_json::to_string(b).unwrap(),"expected":expected})
}
fn error(code: &str, field: Option<&str>) -> Value {
    json!({"code":code,"field":field})
}
fn corpus() -> Value {
    let low = declaration("999.0", 'a', Some(0));
    let high = declaration("arbitrary release 🦀", 'b', Some(u32::MAX));
    let absent = declaration("v\n1", 'c', None);
    let mut cases = vec![
        case("numeric-not-semver", &low, &high, json!("less")),
        case("reverse", &high, &low, json!("greater")),
        case("same-identity-declared", &low, &low, json!("equal")),
        case("same-identity-undeclared", &absent, &absent, json!("equal")),
        case("one-undeclared", &low, &absent, json!("unordered")),
        case("other-undeclared", &absent, &low, json!("unordered")),
        case(
            "both-undeclared",
            &absent,
            &declaration("1", 'd', None),
            json!("unordered"),
        ),
        case(
            "ordinal-tie",
            &low,
            &declaration("something else", 'b', Some(0)),
            json!("equal"),
        ),
        case(
            "control-label-declared",
            &declaration("v\n1", 'c', Some(1)),
            &low,
            json!("greater"),
        ),
        case(
            "same-label-different-digest",
            &low,
            &declaration("999.0", 'b', Some(1)),
            json!("less"),
        ),
        case(
            "contradictory-exact-identity",
            &low,
            &declaration("999.0", 'a', Some(1)),
            error("integrity_failure", Some("version_order")),
        ),
        case(
            "contradictory-absence",
            &low,
            &declaration("999.0", 'a', None),
            error("integrity_failure", Some("version_order")),
        ),
        case(
            "null-is-absent",
            &{
                let mut v = absent.clone();
                v["version_order"] = Value::Null;
                v
            },
            &absent,
            json!("equal"),
        ),
        case(
            "version-byte-boundary",
            &declaration(&"é".repeat(2048), 'b', Some(1)),
            &low,
            json!("greater"),
        ),
    ];
    for (name, field, value, code, path) in [
        (
            "negative",
            "version_order",
            json!(-1),
            "malformed_response",
            None,
        ),
        (
            "fraction",
            "version_order",
            json!(1.5),
            "malformed_response",
            None,
        ),
        (
            "float-integer",
            "version_order",
            json!(1.0),
            "malformed_response",
            None,
        ),
        (
            "string",
            "version_order",
            json!("1"),
            "malformed_response",
            None,
        ),
        (
            "overflow",
            "version_order",
            json!(4294967296u64),
            "malformed_response",
            None,
        ),
        (
            "bool",
            "version_order",
            json!(true),
            "malformed_response",
            None,
        ),
        (
            "unknown-member",
            "unexpected",
            json!(1),
            "malformed_response",
            None,
        ),
    ] {
        let mut changed = low.clone();
        changed[field] = value;
        cases.push(case(name, &changed, &high, error(code, path)));
    }
    for (name, field, value, code, path) in [
        (
            "cross-id",
            "id",
            json!("https://graphforge.sh/ont_bbbbbbbbbbbbbbbb"),
            "invalid_identity",
            "module.id",
        ),
        (
            "bridge-id",
            "id",
            json!("https://graphforge.sh/brg_aaaaaaaaaaaaaaaa"),
            "invalid_identity",
            "module.id",
        ),
        (
            "bare-id",
            "id",
            json!("ont_aaaaaaaaaaaaaaaa"),
            "invalid_identity",
            "module.id",
        ),
        (
            "uppercase-uri",
            "id",
            json!("https://graphforge.sh/ONT_AAAAAAAAAAAAAAAA"),
            "invalid_identity",
            "module.id",
        ),
        (
            "bad-digest",
            "content_digest",
            json!("sha256:bad"),
            "integrity_failure",
            "digest",
        ),
        (
            "empty-version",
            "version",
            json!(""),
            "limit_exceeded",
            "module.version",
        ),
        (
            "version-byte-overflow",
            "version",
            json!("é".repeat(2048) + "x"),
            "limit_exceeded",
            "module.version",
        ),
    ] {
        let mut changed = low.clone();
        changed["module"][field] = value;
        cases.push(case(name, &changed, &high, error(code, Some(path))));
    }
    let text = serde_json::to_string(&low).unwrap();
    for (name, text) in [
        (
            "duplicate-order",
            text.replace(
                "\"version_order\":",
                "\"version_order\":2,\"version_order\":",
            ),
        ),
        (
            "duplicate-nested",
            text.replace("\"version\":", "\"version\":\"ignored\",\"version\":"),
        ),
        (
            "unknown-nested",
            text.replace("\"version\":", "\"extra\":1,\"version\":"),
        ),
        ("trailing-json", format!("{text} null")),
        ("malformed-json", "{".into()),
    ] {
        cases.push(raw_case(
            name,
            &text,
            &high,
            error("malformed_response", None),
        ));
    }
    let mut bounded = case(
        "aggregate-bound",
        &low,
        &high,
        error("limit_exceeded", Some("module_versions")),
    );
    bounded["limits"] = json!({"max_response_bytes":text.len()});
    cases.push(bounded);
    let mut bounded = case(
        "version-input-bound",
        &declaration("abcdef", 'b', Some(1)),
        &low,
        error("limit_exceeded", Some("module.version")),
    );
    bounded["limits"] = json!({"max_string_bytes":50});
    // URI fits the limit; version alone exceeds it.
    bounded["a"] =
        json!(serde_json::to_string(&declaration(&"a".repeat(51), 'b', Some(1))).unwrap());
    cases.push(bounded);
    json!({"format":"graphforge-module-version-order-conformance/1","cases":cases})
}

#[test]
fn module_version_order_artifact_and_native_comparison() {
    let corpus = corpus();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/reference/discovery/v1/module-version-order-conformance.json");
    let bytes = format!("{}\n", serde_json::to_string_pretty(&corpus).unwrap());
    if std::env::var_os("GRAPHFORGE_UPDATE_DISCOVERY_ARTIFACTS").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    } else {
        assert_eq!(std::fs::read(path).unwrap(), bytes.as_bytes());
    }
    for case in corpus["cases"].as_array().unwrap() {
        let limits = DiscoveryLimits {
            max_response_bytes: case["limits"]["max_response_bytes"]
                .as_u64()
                .map_or(DiscoveryLimits::default().max_response_bytes, |n| {
                    n as usize
                }),
            max_string_bytes: case["limits"]["max_string_bytes"]
                .as_u64()
                .map_or(DiscoveryLimits::default().max_string_bytes, |n| n as usize),
            ..DiscoveryLimits::default()
        };
        let a = case["a"].as_str().unwrap().as_bytes();
        let b = case["b"].as_str().unwrap().as_bytes();
        let outcome = compare_module_versions_json(a, b, limits);
        match outcome {
            Ok(order) => {
                assert_eq!(
                    serde_json::to_value(order).unwrap(),
                    case["expected"],
                    "{}",
                    case["name"]
                );
                let a = DeclaredModuleVersion::from_json(a, limits).unwrap();
                let b = DeclaredModuleVersion::from_json(b, limits).unwrap();
                assert_eq!(compare_module_versions(&a, &b, limits).unwrap(), order);
            }
            Err(error) => assert_eq!(
                serde_json::to_value(error).unwrap(),
                case["expected"],
                "{}",
                case["name"]
            ),
        }
    }
}

#[test]
fn typed_comparison_checks_bounds_and_identity_before_returning_equal() {
    let limits = DiscoveryLimits::default();
    let good: DeclaredModuleVersion = serde_json::from_value(declaration("1", 'a', None)).unwrap();
    for (field, value, code) in [
        ("id", json!("ont_aaaaaaaaaaaaaaaa"), "invalid_identity"),
        ("version", json!(""), "limit_exceeded"),
        ("content_digest", json!("sha256:bad"), "integrity_failure"),
    ] {
        let mut bad = declaration("1", 'a', None);
        bad["module"][field] = value;
        let bad: DeclaredModuleVersion = serde_json::from_value(bad).unwrap();
        let error = compare_module_versions(&bad, &bad, limits).unwrap_err();
        assert_eq!(serde_json::to_value(error.code).unwrap(), code);
    }
    let limits = DiscoveryLimits {
        max_response_bytes: 1,
        ..limits
    };
    assert!(compare_module_versions(&good, &good, limits).is_err());
}
