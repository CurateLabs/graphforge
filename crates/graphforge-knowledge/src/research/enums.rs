//! Frozen closed spellings shared by Arrow and canonical content.
use super::{ClaimRelationKind, ResearchCategory, ResearchDecisionKind, ResearchSubjectKind};
use crate::{KnowledgeError, invalid};
impl ResearchCategory {
    /// Exact persisted contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MachineExtraction => "machine_extraction",
            Self::AnalystAssertion => "analyst_assertion",
            Self::Interpretation => "interpretation",
            Self::Hypothesis => "hypothesis",
            Self::Theory => "theory",
            Self::Annotation => "annotation",
        }
    }
    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "machine_extraction" => Ok(Self::MachineExtraction),
            "analyst_assertion" => Ok(Self::AnalystAssertion),
            "interpretation" => Ok(Self::Interpretation),
            "hypothesis" => Ok(Self::Hypothesis),
            "theory" => Ok(Self::Theory),
            "annotation" => Ok(Self::Annotation),
            _ => Err(invalid(
                "ResearchCategory",
                "unsupported closed registry value",
            )),
        }
    }
}
impl ClaimRelationKind {
    /// Exact persisted contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AlternativeTo => "alternative_to",
            Self::Contradicts => "contradicts",
            Self::Disputes => "disputes",
            Self::Refines => "refines",
            Self::Supersedes => "supersedes",
            Self::Supports => "supports",
        }
    }
    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "alternative_to" => Ok(Self::AlternativeTo),
            "contradicts" => Ok(Self::Contradicts),
            "disputes" => Ok(Self::Disputes),
            "refines" => Ok(Self::Refines),
            "supersedes" => Ok(Self::Supersedes),
            "supports" => Ok(Self::Supports),
            _ => Err(invalid(
                "ClaimRelationKind",
                "unsupported closed registry value",
            )),
        }
    }
}
impl ResearchSubjectKind {
    /// Exact persisted contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Edge => "edge",
            Self::Assertion => "assertion",
        }
    }
    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "node" => Ok(Self::Node),
            "edge" => Ok(Self::Edge),
            "assertion" => Ok(Self::Assertion),
            _ => Err(invalid(
                "ResearchSubjectKind",
                "unsupported closed registry value",
            )),
        }
    }
}
impl ResearchDecisionKind {
    /// Exact persisted contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Integrate => "integrate",
            Self::Promote => "promote",
            Self::Revoke => "revoke",
        }
    }
    pub(super) fn parse(value: &str) -> Result<Self, KnowledgeError> {
        match value {
            "integrate" => Ok(Self::Integrate),
            "promote" => Ok(Self::Promote),
            "revoke" => Ok(Self::Revoke),
            _ => Err(invalid(
                "ResearchDecisionKind",
                "unsupported closed registry value",
            )),
        }
    }
}
