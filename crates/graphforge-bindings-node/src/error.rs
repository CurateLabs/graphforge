//! `GfError` → JS error mapping for the Node binding.
//!
//! napi-rs cannot export JS `Error` subclasses, so the fault domain is carried
//! on `err.code` (the napi error `status` string becomes the JS `error.code`).
//! `ParseError` and `Bind` (binder failures) additionally encode their source
//! span as a leading `[span:<start>:<len>]` token in the message — mirroring
//! the Python binding's `(offset, length)` tuple on the exception's `span`
//! (`crates/graphforge-bindings-py`'s `to_pyerr`). Binder failures share the
//! public `ParseError` / `GF_PARSE` domain with lexer/grammar parse failures.

use graphforge_api::GfError;
use napi::Env;

/// A napi error whose `status` string surfaces as the JS `error.code`.
pub type NodeError = napi::Error<String>;

/// Throw a real JavaScript `TypeError`, then return a `PendingException` sentinel
/// so the `#[napi]` wrapper does not overwrite it with a generic `Error`.
///
/// Use this for binding coercion failures (bad handles, unsupported property
/// types). Do **not** use it for Rust-owned [`GfError`] domains — those continue
/// to go through [`to_napi_err`].
pub fn type_error(env: Env, message: impl Into<String>) -> NodeError {
    let message = message.into();
    let _ = env.throw_type_error(&message, Some("InvalidArg"));
    napi::Error::new("PendingException".to_owned(), message)
}

/// The JS `error.code` for each [`GfError`] fault domain.
fn error_code(err: &GfError) -> &'static str {
    match err {
        GfError::Parse { .. } | GfError::Bind { .. } => "ParseError",
        GfError::Plan(_) => "PlanError",
        GfError::Execution(_) | GfError::Provider { .. } => "ExecutionError",
        GfError::Storage(_) => "GF_IO",
        GfError::Project { code, .. } => code.as_str(),
        GfError::Api { code, .. } => code.as_str(),
        GfError::Lifecycle(_) => "LifecycleError",
        GfError::Validation(message)
            if is_uuid_parameter_validation(message)
                || is_bulk_validation(message)
                || is_ontology_lifecycle_validation(message) =>
        {
            "GF_VALIDATION"
        }
        GfError::Validation(_) => "ValidationError",
        GfError::Ontology(_) => "OntologyError",
        GfError::NotImplemented(_) => "NotImplementedError",
    }
}

fn is_bulk_validation(message: &str) -> bool {
    message.starts_with("GF_BULK_VALIDATION(")
}

fn is_ontology_lifecycle_validation(message: &str) -> bool {
    matches!(
        message,
        "ontology_id and version must be non-empty"
            | "ontology mode must be advisory or strict"
            | "ontology export format must be yaml or json"
            | "ontology export source must be suggested, loaded, or adopted"
            | "document is required for suggested ontology export"
    ) || message.starts_with("invalid ontology document: ")
}

fn is_uuid_parameter_validation(message: &str) -> bool {
    matches!(
        message,
        "UUID parameter must be canonical hyphenated UUID text"
            | "UUID parameter tag must contain only $uuid"
            | "UUID parameter $uuid value must be a string"
    ) || matches_named_template(
        message,
        "typed UUID parameter `$",
        "` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
    ) || matches_named_template(
        message,
        "property `",
        "` cannot store typed UUID query parameters",
    )
}

fn matches_named_template(message: &str, prefix: &str, suffix: &str) -> bool {
    message
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix(suffix))
        .is_some_and(|name| !name.is_empty() && !name.contains('`'))
}

/// Map a [`GfError`] to a [`NodeError`] carrying the fault-domain `code`.
///
/// `ParseError` prefixes its message with `[span:<start>:<len>]` so callers can
/// recover the source span (napi cannot attach a structured `span` property).
pub fn to_napi_err(err: &GfError) -> NodeError {
    let message = match err {
        GfError::Parse { msg, span } | GfError::Bind { msg, span } => format!(
            "[span:{}:{}] {}",
            span.start,
            span.end.saturating_sub(span.start),
            msg
        ),
        GfError::Plan(m)
        | GfError::Execution(m)
        | GfError::Storage(m)
        | GfError::Lifecycle(m)
        | GfError::Validation(m)
        | GfError::Ontology(m) => m.clone(),
        GfError::Project { message, .. } | GfError::Api { message, .. } => message.clone(),
        GfError::Provider {
            class,
            provider,
            model,
        } => format!("provider invocation failed: class={class} provider={provider} model={model}"),
        GfError::NotImplemented(name) => (*name).to_owned(),
    };
    napi::Error::new(error_code(err).to_owned(), message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_api::Span;

    #[test]
    fn maps_each_variant_to_its_code() {
        let cases: Vec<(GfError, &str)> = vec![
            (GfError::Plan("p".into()), "PlanError"),
            (GfError::Execution("e".into()), "ExecutionError"),
            (
                GfError::Provider {
                    class: "timeout".into(),
                    provider: "openrouter".into(),
                    model: "vendor/model".into(),
                },
                "ExecutionError",
            ),
            (GfError::Storage("s".into()), "GF_IO"),
            (
                GfError::Project {
                    code: graphforge_api::ProjectErrorCode::UnsupportedProjectFormat,
                    message: "unsupported".into(),
                },
                "GF_UNSUPPORTED_PROJECT_FORMAT",
            ),
            (GfError::Lifecycle("l".into()), "LifecycleError"),
            (GfError::Validation("v".into()), "ValidationError"),
            (GfError::Ontology("o".into()), "OntologyError"),
            (GfError::NotImplemented("rank"), "NotImplementedError"),
        ];
        for (err, code) in &cases {
            let mapped = to_napi_err(err);
            assert_eq!(&mapped.status, code, "wrong code for {err:?}");
            if *code == "GF_UNSUPPORTED_PROJECT_FORMAT" {
                assert_eq!(mapped.reason, "unsupported");
            }
        }
    }

    #[test]
    fn ontology_lifecycle_validation_preserves_the_rust_code() {
        for message in [
            "ontology_id and version must be non-empty",
            "ontology mode must be advisory or strict",
            "ontology export format must be yaml or json",
            "ontology export source must be suggested, loaded, or adopted",
            "document is required for suggested ontology export",
            "invalid ontology document: missing field `ontology_id`",
        ] {
            let error = GfError::Validation(message.into());
            assert_eq!(error_code(&error), error.code());
        }
    }

    #[test]
    fn preserves_the_message() {
        let mapped = to_napi_err(&GfError::Validation("empty query".into()));
        assert_eq!(mapped.reason, "empty query");
    }

    #[test]
    fn uuid_parameter_validation_uses_the_stable_public_code() {
        for message in [
            "UUID parameter must be canonical hyphenated UUID text",
            "UUID parameter tag must contain only $uuid",
            "UUID parameter $uuid value must be a string",
            "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
            "property `value` cannot store typed UUID query parameters",
        ] {
            let mapped = to_napi_err(&GfError::Validation(message.into()));
            assert_eq!(mapped.status, "GF_VALIDATION");
            assert_eq!(mapped.reason, message);
        }
    }

    #[test]
    fn bulk_validation_uses_the_stable_public_code() {
        let message =
            "GF_BULK_VALIDATION(invalid_uuid): bulk node row 0 field \"node_uuid\": invalid UUID";
        let mapped = to_napi_err(&GfError::Validation(message.into()));
        assert_eq!(mapped.status, "GF_VALIDATION");
        assert_eq!(mapped.reason, message);
    }

    #[test]
    fn uuid_parameter_validation_does_not_capture_lookalike_messages() {
        for message in [
            "UUID parameter validation failed",
            "prefix: UUID parameter must be canonical hyphenated UUID text",
            "UUID parameter must be canonical hyphenated UUID text (extra)",
            "typed UUID parameter `$id` is invalid",
            "typed UUID parameter `$` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
            "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate (extra)",
            "cannot store typed UUID query parameters",
            "property `` cannot store typed UUID query parameters",
            "prefix property `value` cannot store typed UUID query parameters",
            "property `value` cannot store typed UUID query parameters (extra)",
        ] {
            let mapped = to_napi_err(&GfError::Validation(message.into()));
            assert_eq!(mapped.status, "ValidationError", "message={message}");
            assert_eq!(mapped.reason, message);
        }
    }

    #[test]
    fn not_implemented_message_is_the_feature_name() {
        let mapped = to_napi_err(&GfError::NotImplemented("rank"));
        assert_eq!(mapped.reason, "rank");
    }

    #[test]
    fn parse_error_encodes_span_offset_and_length() {
        let mapped = to_napi_err(&GfError::Parse {
            msg: "unexpected token".into(),
            span: Span { start: 4, end: 9 },
        });
        assert_eq!(mapped.status, "ParseError");
        // [span:<start>:<len>] with len = end - start (4..9 → offset 4, length 5).
        assert_eq!(mapped.reason, "[span:4:5] unexpected token");
    }

    #[test]
    fn bind_error_shares_parse_domain_and_encodes_span() {
        let mapped = to_napi_err(&GfError::Bind {
            msg: "bind error: variable not in scope".into(),
            span: Span { start: 7, end: 8 },
        });
        // Bind shares the ParseError fault domain and carries a span like Parse.
        assert_eq!(mapped.status, "ParseError");
        assert_eq!(
            mapped.reason,
            "[span:7:1] bind error: variable not in scope"
        );
    }
}
