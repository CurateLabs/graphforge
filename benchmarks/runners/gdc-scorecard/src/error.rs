//! Typed conversion failures. A cause is a stable machine-readable string.

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cause {
    InvalidMapping,
    InputMissing,
    MalformedInput,
    MissingColumn,
    InvalidValue,
    DuplicateNodeIdentity,
    DanglingEndpoint,
    OutputExists,
    Io,
}

impl Cause {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidMapping => "invalid_mapping",
            Self::InputMissing => "input_missing",
            Self::MalformedInput => "malformed_input",
            Self::MissingColumn => "missing_column",
            Self::InvalidValue => "invalid_value",
            Self::DuplicateNodeIdentity => "duplicate_node_identity",
            Self::DanglingEndpoint => "dangling_endpoint",
            Self::OutputExists => "output_exists",
            Self::Io => "io_error",
        }
    }
}

#[derive(Debug)]
pub struct ConvertError {
    cause: Cause,
    message: String,
}

impl ConvertError {
    #[must_use]
    pub fn new(cause: Cause, message: impl Into<String>) -> Self {
        Self {
            cause,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn cause(&self) -> Cause {
        self.cause
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConvertError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.cause.as_str(), self.message)
    }
}

impl std::error::Error for ConvertError {}

pub(crate) fn io_error(context: &str, error: &std::io::Error) -> ConvertError {
    ConvertError::new(Cause::Io, format!("{context}: {error}"))
}
