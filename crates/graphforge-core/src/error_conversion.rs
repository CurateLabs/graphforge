//! Recover GraphForge's structured errors at foreign error boundaries.

use std::error::Error;

use crate::GfError;

impl GfError {
    /// Preserve a GraphForge source when a planner wraps it in another error.
    ///
    /// Foreign failures without a GraphForge source retain the existing
    /// `GF_PLAN` classification and diagnostic. Source traversal uses Rust's
    /// error identity, never diagnostic text.
    #[must_use]
    pub fn from_plan_error(error: impl Error + 'static) -> Self {
        Self::recover_source(&error, false).unwrap_or_else(|| Self::Plan(error.to_string()))
    }

    /// Preserve a GraphForge source when execution wraps it in another error.
    ///
    /// This also handles shared foreign errors: cloning the GraphForge value
    /// retains its typed code, variant, message, and source span without needing
    /// exclusive ownership of an upstream error. Foreign failures without a
    /// GraphForge source retain `GF_EXECUTION` and their existing diagnostic.
    #[must_use]
    pub fn from_execution_error(error: impl Error + 'static) -> Self {
        Self::recover_source(&error, true).unwrap_or_else(|| Self::Execution(error.to_string()))
    }

    fn recover_source(error: &(dyn Error + 'static), execution: bool) -> Option<Self> {
        let mut source = Some(error);
        while let Some(error) = source {
            if let Some(original) = error.downcast_ref::<Self>() {
                return Some(original.clone());
            }
            if let Some(original) = error.downcast_ref::<crate::LoweringError>() {
                return Some(if execution {
                    Self::LoweringExecution(original.clone())
                } else {
                    Self::Lowering(original.clone())
                });
            }
            if let Some(original) = error.downcast_ref::<crate::AlgorithmError>() {
                return Some(Self::Algorithm(original.clone()));
            }
            source = error.source();
        }
        None
    }
}

impl From<crate::ParseError> for GfError {
    fn from(error: crate::ParseError) -> Self {
        Self::Parse {
            msg: error.message.clone(),
            span: error.span,
            diagnostic: Some(Box::new(error)),
        }
    }
}

impl From<crate::LoweringError> for GfError {
    fn from(error: crate::LoweringError) -> Self {
        Self::Lowering(error)
    }
}

impl From<crate::AlgorithmError> for GfError {
    fn from(error: crate::AlgorithmError) -> Self {
        Self::Algorithm(error)
    }
}

impl GfError {
    /// Preserve the legacy EXPLAIN display while retaining the full parser diagnostic.
    #[must_use]
    pub fn from_parse_display(error: crate::ParseError) -> Self {
        Self::Parse {
            msg: error.to_string(),
            span: error.span,
            diagnostic: Some(Box::new(error)),
        }
    }

    /// Preserve legacy cypher EXPLAIN's planning domain and message.
    #[must_use]
    pub fn from_bind_plan_errors(errors: &[crate::BindError]) -> Self {
        Self::BindPlan {
            msg: format!(
                "bind errors: {}",
                errors
                    .iter()
                    .map(|e| e.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            diagnostics: errors.to_vec(),
        }
    }

    /// Preserve every binder diagnostic and the established primary span/message.
    #[must_use]
    pub fn from_bind_errors(errors: &[crate::BindError]) -> Self {
        // This existing UUID validation policy is shared with parser clients.
        if let Some(error) = errors
            .iter()
            .filter(|error| {
                error.kind == crate::BindErrorKind::InvalidArgument
                    && error.message.starts_with("typed UUID parameter `$")
            })
            .min_by_key(|error| (error.span.start, error.message.as_str()))
        {
            return Self::BindValidation {
                msg: error.message.clone(),
                diagnostics: errors.to_vec(),
            };
        }
        Self::Bind {
            msg: errors
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join("; "),
            span: errors.first().map_or(crate::Span::default(), |e| e.span),
            diagnostics: errors.to_vec(),
        }
    }
}

#[cfg(test)]
mod stage_tests {
    use super::*;
    use crate::{BindError, BindErrorKind, LoweringError, ParseError, ParseErrorKind, Span};

    #[test]
    fn parser_kinds_payloads_and_messages_survive_both_presentations() {
        let kinds = [
            ParseErrorKind::UnexpectedChar,
            ParseErrorKind::UnexpectedToken {
                found: "token".into(),
                expected: vec!["expression".into()],
            },
            ParseErrorKind::UnterminatedString,
            ParseErrorKind::UnterminatedBlockComment,
            ParseErrorKind::InvalidNumericLiteral,
            ParseErrorKind::InvalidParameter,
            ParseErrorKind::UnexpectedEof {
                expected: vec!["expression".into()],
            },
        ];
        for kind in kinds {
            let original = ParseError::new(kind, Span::new(7, 13), "detailed source diagnostic");
            for display in [false, true] {
                let error = if display {
                    GfError::from_parse_display(original.clone())
                } else {
                    original.clone().into()
                };
                assert_eq!(error.code(), "GF_PARSE");
                let GfError::Parse {
                    diagnostic,
                    msg,
                    span,
                } = error
                else {
                    panic!("parser variant lost")
                };
                assert_eq!(*diagnostic.expect("typed parser diagnostic"), original);
                assert_eq!(span, original.span);
                assert_eq!(
                    msg,
                    if display {
                        original.to_string()
                    } else {
                        original.message.clone()
                    }
                );
            }
        }
    }

    #[test]
    fn every_binder_kind_and_span_survives_in_reported_order() {
        use BindErrorKind as K;
        let kinds = [
            K::UnknownLabel,
            K::UnknownRelationType,
            K::UnknownProperty,
            K::UndeclaredVariable,
            K::DuplicateVariable,
            K::AmbiguousProperty,
            K::UnsupportedClause,
            K::InvalidDeleteTarget,
            K::VariableKindConflict,
            K::VariableAlreadyBound,
            K::InvalidArgument,
            K::AmbiguousComposedSymbol,
            K::CompositionConflict,
        ];
        let original: Vec<_> = kinds
            .into_iter()
            .enumerate()
            .map(|(i, kind)| {
                BindError::new(kind, Span::new(i * 2, i * 2 + 1), format!("diagnostic {i}"))
            })
            .collect();
        let error = GfError::from_bind_errors(&original);
        assert_eq!(error.code(), "GF_PARSE");
        let GfError::Bind {
            diagnostics,
            msg,
            span,
        } = error
        else {
            panic!("binder variant lost")
        };
        assert_eq!(diagnostics, original);
        assert_eq!(span, original[0].span);
        assert_eq!(
            msg,
            original
                .iter()
                .map(|e| e.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    #[test]
    fn uuid_validation_keeps_all_binder_diagnostics() {
        let original = vec![
            BindError::new(
                BindErrorKind::UnknownLabel,
                Span::new(0, 2),
                "unknown label",
            ),
            BindError::new(
                BindErrorKind::InvalidArgument,
                Span::new(4, 9),
                "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
            ),
        ];
        let error = GfError::from_bind_errors(&original);
        assert_eq!(error.code(), "GF_VALIDATION");
        let GfError::BindValidation { diagnostics, msg } = error else {
            panic!("validation classification lost")
        };
        assert_eq!(diagnostics, original);
        assert_eq!(msg, original[1].message);
    }

    #[test]
    fn lowering_kind_and_runtime_domain_are_preserved() {
        for original in [
            LoweringError::UnknownFunction("fn".into()),
            LoweringError::UnsupportedExpr("shape".into()),
            LoweringError::UnboundVar(42),
            LoweringError::InvalidType("predicate".into()),
        ] {
            let expected = if matches!(original, LoweringError::InvalidType(_)) {
                "GF_VALIDATION"
            } else {
                "GF_PLAN"
            };
            let planning = GfError::from_plan_error(original.clone());
            assert_eq!(planning.code(), expected);
            assert!(matches!(planning, GfError::Lowering(ref e) if e == &original));
            let execution = GfError::from_execution_error(original.clone());
            assert_eq!(execution.code(), "GF_EXECUTION");
            assert!(matches!(execution, GfError::LoweringExecution(ref e) if e == &original));
        }
    }
}
